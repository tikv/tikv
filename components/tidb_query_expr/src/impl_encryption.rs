// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Read;

use byteorder::{ByteOrder, LittleEndian};
use flate2::{
    read::{ZlibDecoder, ZlibEncoder},
    Compression,
};
use openssl::hash::{self, MessageDigest};
use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::{
    codec::data_type::*,
    expr::{Error, EvalContext},
};

const SHA0: i64 = 0;
const SHA224: i64 = 224;
const SHA256: i64 = 256;
const SHA384: i64 = 384;
const SHA512: i64 = 512;

const MAX_RAND_BYTES_LENGTH: i64 = 1024;

#[rpn_fn(nullable)]
#[inline]
pub fn md5(arg: Option<BytesRef>) -> Result<Option<Bytes>> {
    match arg {
        Some(arg) => hex_digest(MessageDigest::md5(), arg).map(Some),
        None => Ok(None),
    }
}

#[rpn_fn(nullable)]
#[inline]
pub fn sha1(arg: Option<BytesRef>) -> Result<Option<Bytes>> {
    match arg {
        Some(arg) => hex_digest(MessageDigest::sha1(), arg).map(Some),
        None => Ok(None),
    }
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn sha2(
    ctx: &mut EvalContext,
    input: Option<BytesRef>,
    hash_length: Option<&Int>,
) -> Result<Option<Bytes>> {
    match (input, hash_length) {
        (Some(input), Some(hash_length)) => {
            let sha2 = match *hash_length {
                SHA0 | SHA256 => MessageDigest::sha256(),
                SHA224 => MessageDigest::sha224(),
                SHA384 => MessageDigest::sha384(),
                SHA512 => MessageDigest::sha512(),
                _ => {
                    ctx.warnings
                        .append_warning(Error::incorrect_parameters("sha2"));
                    return Ok(None);
                }
            };
            hex_digest(sha2, input).map(Some)
        }
        _ => Ok(None),
    }
}

#[rpn_fn(writer)]
#[inline]
pub fn compress(input: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    // compress implements the `COMPRESS` built-in function.
    // MySQL doc: https://dev.mysql.com/doc/refman/5.6/en/encryption-functions.html#function_compress
    // according to MySQL doc: Empty strings are stored as empty strings.
    if input.is_empty() {
        return Ok(writer.write_ref(Some(b"")));
    }
    let mut e = ZlibEncoder::new(input, Compression::default());
    // preferred capacity is input length plus four bytes length header and one extra end "."
    // max capacity is isize::max_value(), or will panic with "capacity overflow"
    let mut vec = Vec::with_capacity((input.len() + 5).min(isize::max_value() as usize));
    vec.resize(4, 0);
    LittleEndian::write_u32(&mut vec, input.len() as u32);
    match e.read_to_end(&mut vec) {
        Ok(_) => {
            // according to MySQL doc: append "." if ends with space
            if *vec.last().unwrap() == b' ' {
                vec.push(b'.');
            }
            Ok(writer.write_ref(Some(vec.as_ref())))
        }
        _ => Ok(writer.write(None)),
    }
}

#[rpn_fn(writer, capture = [ctx])]
#[inline]
pub fn uncompress(
    ctx: &mut EvalContext,
    input: BytesRef,
    writer: BytesWriter,
) -> Result<BytesGuard> {
    // uncompressed implements the `UNCOMPRESS` built-in function.
    // MySQL doc: https://dev.mysql.com/doc/refman/5.6/en/encryption-functions.html#function_uncompress
    // according to MySQL doc: Empty strings are stored as empty strings.
    if input.is_empty() {
        return Ok(writer.write_ref(Some(b"")));
    }
    if input.len() <= 4 {
        ctx.warnings.append_warning(Error::zlib_data_corrupted());
        return Ok(writer.write(None));
    }

    let len = LittleEndian::read_u32(&input[0..4]) as usize;
    let mut d = ZlibDecoder::new(&input[4..]);
    let mut vec = Vec::with_capacity(len);

    // if the length of uncompressed string is greater than the length we read from the first
    //     four bytes, return null and generate a length corrupted warning.
    // if the length of uncompressed string is zero or uncompress fail, return null and generate
    //     a data corrupted warning
    match d.read_to_end(&mut vec) {
        Ok(decoded_len) if len >= decoded_len && decoded_len != 0 => {
            Ok(writer.write_ref(Some(vec.as_ref())))
        }
        Ok(decoded_len) if len < decoded_len => {
            ctx.warnings.append_warning(Error::zlib_length_corrupted());
            Ok(writer.write(None))
        }
        _ => {
            ctx.warnings.append_warning(Error::zlib_data_corrupted());
            Ok(writer.write(None))
        }
    }
}

// https://dev.mysql.com/doc/refman/5.7/en/password-hashing.html
#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn password(ctx: &mut EvalContext, input: Option<BytesRef>) -> Result<Option<Bytes>> {
    ctx.warnings.append_warning(Error::Other(box_err!(
        "Warning: Deprecated syntax PASSWORD"
    )));
    match input {
        Some(bytes) => {
            if bytes.is_empty() {
                Ok(Some(Vec::new()))
            } else {
                let hash1 = hex_digest(MessageDigest::sha1(), bytes)?;
                let mut hash2 = hex_digest(MessageDigest::sha1(), hash1.as_slice())?;
                hash2.insert(0, b'*');
                Ok(Some(hash2))
            }
        }
        None => Ok(None),
    }
}

#[inline]
fn hex_digest(hashtype: MessageDigest, input: &[u8]) -> Result<Bytes> {
    hash::hash(hashtype, input)
        .map(|digest| hex::encode(digest).into_bytes())
        .map_err(|e| box_err!("OpenSSL error: {:?}", e))
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn uncompressed_length(ctx: &mut EvalContext, arg: Option<BytesRef>) -> Result<Option<Int>> {
    use byteorder::{ByteOrder, LittleEndian};
    Ok(arg.as_ref().map(|s| {
        if s.is_empty() {
            0
        } else if s.len() <= 4 {
            ctx.warnings.append_warning(Error::zlib_data_corrupted());
            0
        } else {
            Int::from(LittleEndian::read_u32(&s[0..4]))
        }
    }))
}

#[rpn_fn(nullable, capture = [ctx])]
#[inline]
pub fn random_bytes(_ctx: &mut EvalContext, arg: Option<&Int>) -> Result<Option<Bytes>> {
    match arg {
        Some(arg) => {
            if *arg < 1 || *arg > MAX_RAND_BYTES_LENGTH {
                return Err(Error::overflow("length", "random_bytes").into());
            }
            Ok(Some(
                (0..*arg as usize).map(|_| rand::random::<u8>()).collect(),
            ))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use tipb::ScalarFuncSig;

    use super::*;
    use crate::types::test_util::RpnFnScalarEvaluator;

    fn test_unary_func_ok_none<'a, I: EvaluableRef<'a>, O: EvaluableRet>(sig: ScalarFuncSig)
    where
        O: PartialEq,
        Option<I>: Into<ScalarValue>,
        Option<O>: From<ScalarValue>,
    {
        assert_eq!(
            None,
            RpnFnScalarEvaluator::new()
                .push_param(Option::<I>::None)
                .evaluate::<O>(sig)
                .unwrap()
        );
    }

    #[test]
    fn test_md5() {
        let test_cases = vec![
            (vec![], "d41d8cd98f00b204e9800998ecf8427e"),
            (b"a".to_vec(), "0cc175b9c0f1b6a831c399e269772661"),
            (b"ab".to_vec(), "187ef4436122d1cc2f40dc2b92f0eba0"),
            (b"abc".to_vec(), "900150983cd24fb0d6963f7d28e17f72"),
            (b"123".to_vec(), "202cb962ac59075b964b07152d234b70"),
            (
                "你好".as_bytes().to_vec(),
                "7eca689f0d3389d9dea66ae112e5cfd7",
            ),
            (
                "分布式データベース".as_bytes().to_vec(),
                "63c0354797bd261e2cbf8581147eeeda",
            ),
            (vec![0xc0, 0x80], "b26555f33aedac7b2684438cc5d4d05e"),
            (vec![0xED, 0xA0, 0x80], "546d3dc8de10fbf8b448f678a47901e4"),
        ];
        for (arg, expect_output) in test_cases {
            let expect_output = Some(Bytes::from(expect_output));

            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Bytes>(ScalarFuncSig::Md5)
                .unwrap();
            assert_eq!(output, expect_output);
        }
        test_unary_func_ok_none::<BytesRef, Bytes>(ScalarFuncSig::Md5);
    }

    #[test]
    fn test_sha1() {
        let test_cases = vec![
            (vec![], "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            (b"a".to_vec(), "86f7e437faa5a7fce15d1ddcb9eaeaea377667b8"),
            (b"ab".to_vec(), "da23614e02469a0d7c7bd1bdab5c9c474b1904dc"),
            (b"abc".to_vec(), "a9993e364706816aba3e25717850c26c9cd0d89d"),
            (b"123".to_vec(), "40bd001563085fc35165329ea1ff5c5ecbdbbeef"),
            (
                "你好".as_bytes().to_vec(),
                "440ee0853ad1e99f962b63e459ef992d7c211722",
            ),
            (
                "分布式データベース".as_bytes().to_vec(),
                "82aa64080df2ca37550ddfc3419d75ac1df3e0d0",
            ),
            (vec![0xc0, 0x80], "8bf4822782a21d7ac68ece130ac36987548003bd"),
            (
                vec![0xED, 0xA0, 0x80],
                "10db70ec072d000c68dd95879f9b831e43a859fd",
            ),
        ];
        for (arg, expect_output) in test_cases {
            let expect_output = Some(Bytes::from(expect_output));

            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Bytes>(ScalarFuncSig::Sha1)
                .unwrap();
            assert_eq!(output, expect_output);
        }
        test_unary_func_ok_none::<BytesRef, Bytes>(ScalarFuncSig::Sha1);
    }

    #[test]
    fn test_uncompressed_length() {
        let cases = vec![
            (Some(""), Some(0)),
            (
                Some("0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D"),
                Some(11),
            ),
            (
                Some("0C000000789CCB48CDC9C95728CF2F32303402001D8004202E"),
                Some(12),
            ),
            (Some("020000000000"), Some(2)),
            (Some("0000000001"), Some(0)),
            (
                Some("02000000789CCB48CDC9C95728CF2FCA4901001A0B045D"),
                Some(2),
            ),
            (Some("010203"), Some(0)),
            (Some("01020304"), Some(0)),
            (None, None),
        ];

        for (s, exp) in cases {
            let s = s.map(|inner| hex::decode(inner.as_bytes()).unwrap());
            let output = RpnFnScalarEvaluator::new()
                .push_param(s)
                .evaluate(ScalarFuncSig::UncompressedLength)
                .unwrap();
            assert_eq!(output, exp);
        }
    }

    #[test]
    #[rustfmt::skip]
    fn test_sha2() {
        let cases = vec![
            ("pingcap", 0, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a"),
            ("pingcap", 224, "cd036dc9bec69e758401379c522454ea24a6327b48724b449b40c6b7"),
            ("pingcap", 256, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a"),
            ("pingcap", 384, "c50955b6b0c7b9919740d956849eedcb0f0f90bf8a34e8c1f4e071e3773f53bd6f8f16c04425ff728bed04de1b63db51"),
            ("pingcap", 512, "ea903c574370774c4844a83b7122105a106e04211673810e1baae7c2ae7aba2cf07465e02f6c413126111ef74a417232683ce7ba210052e63c15fc82204aad80"),
            ("13572468", 0, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe"),
            ("13572468", 224, "8ad67735bbf49576219f364f4640d595357a440358d15bf6815a16e4"),
            ("13572468", 256, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe"),
            ("13572468.123", 384, "3b4ee302435dc1e15251efd9f3982b1ca6fe4ac778d3260b7bbf3bea613849677eda830239420e448e4c6dc7c2649d89"),
            ("13572468.123", 512, "4820aa3f2760836557dc1f2d44a0ba7596333fdb60c8a1909481862f4ab0921c00abb23d57b7e67a970363cc3fcb78b25b6a0d45cdcac0e87aa0c96bc51f7f96"),
        ];

        for (input_str, hash_length_i64, exp_str) in cases {
            let exp = Some(Bytes::from(exp_str));

            let got = RpnFnScalarEvaluator::new()
                .push_param(Some(Bytes::from(input_str)))
                .push_param(Some(Int::from(hash_length_i64)))
                .evaluate::<Bytes>(ScalarFuncSig::Sha2)
                .unwrap();
            assert_eq!(got, exp, "sha2('{:?}', {:?})", input_str, hash_length_i64);
        }

        let null_cases = vec![
            (ScalarValue::Bytes(None), ScalarValue::Int(Some(1))),
            (
                ScalarValue::Bytes(Some(b"13572468".to_vec())),
                ScalarValue::Int(None),
            ),
            (ScalarValue::Bytes(None), ScalarValue::Int(None)),
            (
                ScalarValue::Bytes(Some(b"pingcap".to_vec())),
                ScalarValue::Int(Some(-1)),
            ),
            (
                ScalarValue::Bytes(Some(b"13572468".to_vec())),
                ScalarValue::Int(Some(999)),
            ),
        ];

        for (input_str, hash_length_i64) in null_cases {
            assert!(
                RpnFnScalarEvaluator::new()
                    .push_param(input_str)
                    .push_param(hash_length_i64)
                    .evaluate::<Bytes>(ScalarFuncSig::Sha2)
                    .unwrap()
                    .is_none()
            )
        }
    }

    #[test]
    fn test_compress() {
        let test_cases = vec![
            (
                b"hello world".to_vec(),
                "0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D",
            ),
            (b"".to_vec(), ""),
            (
                b"hello wor012".to_vec(),
                "0C000000789CCB48CDC9C95728CF2F32303402001D8004202E",
            ),
        ];
        for (arg, expect) in test_cases {
            let expect = Some(hex::decode(expect.as_bytes()).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Bytes>(ScalarFuncSig::Compress)
                .unwrap();
            assert_eq!(output, expect);
        }
        test_unary_func_ok_none::<BytesRef, Bytes>(ScalarFuncSig::Compress);
    }

    #[test]
    fn test_uncompress() {
        let cases = vec![
            ("", Some("")),
            (
                "0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D",
                Some("hello world"),
            ),
            (
                "0C000000789CCB48CDC9C95728CF2F32303402001D8004202E",
                Some("hello wor012"),
            ),
            (
                "12000000789CCB48CDC9C95728CF2FCA4901001A0B045D",
                Some("hello world"),
            ),
            ("010203", None),
            ("01020304", None),
            ("020000000000", None),
            ("0000000001", None),
            ("02000000789CCB48CDC9C95728CF2FCA4901001A0B045D", None),
        ];
        for (arg, expect) in cases {
            let arg = hex::decode(arg.as_bytes()).unwrap();
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<Bytes>(ScalarFuncSig::Uncompress)
                .unwrap();
            let expect = expect.map(Bytes::from);
            assert_eq!(output, expect);
        }
        test_unary_func_ok_none::<BytesRef, Bytes>(ScalarFuncSig::Uncompress);
    }

    #[test]
    fn test_random_bytes() {
        let cases = vec![1, 32, 233, 1024];

        for len in cases {
            let got = RpnFnScalarEvaluator::new()
                .push_param(Some(len as i64))
                .evaluate::<Bytes>(ScalarFuncSig::RandomBytes)
                .unwrap();
            assert_eq!(got.unwrap().len(), len);
        }

        let overflow_tests = vec![
            ScalarValue::Int(Some(-32)),
            ScalarValue::Int(Some(1025)),
            ScalarValue::Int(Some(0)),
        ];

        for len in overflow_tests {
            assert!(
                RpnFnScalarEvaluator::new()
                    .push_param(len)
                    .evaluate::<Bytes>(ScalarFuncSig::RandomBytes)
                    .is_err(),
            );
        }

        //test NULL case
        assert!(
            RpnFnScalarEvaluator::new()
                .push_param(ScalarValue::Int(None))
                .evaluate::<Bytes>(ScalarFuncSig::RandomBytes)
                .unwrap()
                .is_none()
        )
    }

    #[test]
    fn test_password() {
        let cases = vec![
            ("TiKV", "*cca644408381f962dba8dfb9889db1371ee74208"),
            ("Pingcap", "*f33bc75eac70ac317621fbbfa560d6251c43cf8a"),
            ("rust", "*090c2b08e0c1776910e777b917c2185be6554c2e"),
            ("database", "*02e86b4af5219d0ba6c974908aea62d42eb7da24"),
            ("raft", "*b23a77787ed44e62ef2570f03ce8982d119fb699"),
        ];

        for (input, output) in cases {
            let res = RpnFnScalarEvaluator::new()
                .push_param(Some(Bytes::from(input)))
                .evaluate::<Bytes>(ScalarFuncSig::Password)
                .unwrap();
            assert_eq!(res, Some(Bytes::from(output)))
        }

        // test for null
        let res = RpnFnScalarEvaluator::new()
            .push_param(ScalarValue::Bytes(None))
            .evaluate::<Bytes>(ScalarFuncSig::Password)
            .unwrap();
        assert_eq!(None, res)
    }
}

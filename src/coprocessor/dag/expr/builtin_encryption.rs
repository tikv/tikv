// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;

use super::{Error, EvalContext, Result, ScalarFunc};
use crate::coprocessor::codec::Datum;
use crypto::{
    digest::Digest,
    md5::Md5,
    sha1::Sha1,
    sha2::{Sha224, Sha256, Sha384, Sha512},
};
use flate2::read::{ZlibDecoder, ZlibEncoder};
use flate2::Compression;
use hex;
use openssl::nid::Nid;
use openssl::symm::{decrypt, encrypt, Cipher};
use phf::phf_set;
use rand::distributions::{Distribution, Uniform};
use std::io::prelude::*;

const SHA0: i64 = 0;
const SHA224: i64 = 224;
const SHA256: i64 = 256;
const SHA384: i64 = 384;
const SHA512: i64 = 512;

/// Numerical identifiers for an OpenSSL object supported in TiKV.
const NID: phf::Set<i32> = phf_set! {
    418i32, // aes-128-ecb
    419i32, // aes-128-cbc
    420i32, // aes-128-ofb128
    421i32, // aes-128-cfb128
    422i32, // aes-192-ecb
    423i32, // aes-192-cbc
    424i32, // aes-192-ofb128
    425i32, // aes-192-cfb128
    426i32, // aes-256-ecb
    427i32, // aes-256-cbc
    428i32, // aes-256-ofb128
    429i32, // aes-256-cfb128
    650i32, // aes-128-cfb1
    651i32, // aes-192-cfb1
    652i32, // aes-256-cfb1
    653i32, // aes-128-cfb8
    654i32, // aes-192-cfb8
    655i32, // aes-256-cfb8
};

/// Generate a sequence of random bytes in length of `len`
fn random_bytes(len: usize) -> Vec<u8> {
    Uniform::from(0..=255)
        .sample_iter(&mut rand::thread_rng())
        .take(len)
        .collect::<Vec<_>>()
}

/// Create AES key from arbitrary length array `key`, padding to `key_size` (in bytes)
/// For more detail:
/// https://github.com/mysql/mysql-server/blob/e4924f36486f971f8a04252e01c803457a2c72f7/router/src/harness/src/my_aes_openssl.cc#L71
fn aes_create_key(key: &[u8], key_size: usize) -> Vec<u8> {
    key.iter()
        .enumerate()
        .fold(vec![0; key_size], |mut rkey, (i, x)| {
            rkey[i & (key_size - 1)] ^= x;
            rkey
        })
}

/// Check is the `cipher` needs an IV,
/// If it does, return Error if IV provided is too short or truncate it with cipher's IV's len.
/// Otherwise, return None
pub fn check_iv(iv: &[u8], cipher: Cipher) -> Result<Option<&[u8]>> {
    match cipher.iv_len() {
        Some(len) if iv.len() < len => Err(Error::overflow("iv's length", "iv is too short")),
        Some(len) => Ok(Some(&iv[..len])),
        _ => Ok(None),
    }
}

impl ScalarFunc {
    pub fn md5<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let mut hasher = Md5::new();
        let mut buff: [u8; 16] = [0; 16];
        hasher.input(input.as_ref());
        hasher.result(&mut buff);
        let md5 = hex::encode(buff).into_bytes();
        Ok(Some(Cow::Owned(md5)))
    }

    pub fn sha1<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let mut hasher = Sha1::new();
        let mut buff: [u8; 20] = [0; 20];
        hasher.input(input.as_ref());
        hasher.result(&mut buff);
        let sha1 = hex::encode(buff).into_bytes();
        Ok(Some(Cow::Owned(sha1)))
    }

    pub fn sha2<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let hash_length = try_opt!(self.children[1].eval_int(ctx, row));

        let sha2 = match hash_length {
            SHA0 | SHA256 => {
                let mut hasher = Sha256::new();
                hasher.input(input.as_ref());
                hasher.result_str().into_bytes()
            }
            SHA224 => {
                let mut hasher = Sha224::new();
                hasher.input(input.as_ref());
                hasher.result_str().into_bytes()
            }
            SHA384 => {
                let mut hasher = Sha384::new();
                hasher.input(input.as_ref());
                hasher.result_str().into_bytes()
            }
            SHA512 => {
                let mut hasher = Sha512::new();
                hasher.input(input.as_ref());
                hasher.result_str().into_bytes()
            }
            _ => return Ok(None),
        };
        Ok(Some(Cow::Owned(sha2)))
    }

    #[inline]
    pub fn compress<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        use byteorder::{ByteOrder, LittleEndian};
        let input = try_opt!(self.children[0].eval_string(ctx, row));

        // according to MySQL doc: Empty strings are stored as empty strings.
        if input.is_empty() {
            return Ok(Some(Cow::Borrowed(b"")));
        }
        let mut e = ZlibEncoder::new(input.as_ref(), Compression::default());
        // preferred capacity is input length plus four bytes length header and one extra end "."
        // max capacity is isize::max_value(), or will panic with "capacity overflow"
        let mut vec = Vec::with_capacity((input.len() + 5).min(isize::max_value() as usize));
        vec.resize(4, 0);
        LittleEndian::write_u32(&mut vec, input.len() as u32);
        match e.read_to_end(&mut vec) {
            Ok(_) => {
                // according to MySQL doc: append "." if ends with space
                if vec[vec.len() - 1] == 32 {
                    vec.push(b'.');
                }
                Ok(Some(Cow::Owned(vec)))
            }
            _ => Ok(None),
        }
    }

    #[inline]
    pub fn uncompress<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        use byteorder::{ByteOrder, LittleEndian};
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        if input.is_empty() {
            return Ok(Some(Cow::Borrowed(b"")));
        }
        if input.len() <= 4 {
            ctx.warnings.append_warning(Error::zlib_data_corrupted());
            return Ok(None);
        }

        let len = LittleEndian::read_u32(&input[0..4]) as usize;
        let mut decoder = ZlibDecoder::new(&input[4..]);
        let mut vec = Vec::with_capacity(len);
        // if the length of uncompressed string is greater than the length we read from the first
        //     four bytes, return null and generate a length corrupted warning.
        // if the length of uncompressed string is zero or uncompress fail, return null and generate
        //     a data corrupted warning
        match decoder.read_to_end(&mut vec) {
            Ok(decoded_len) if len >= decoded_len && decoded_len != 0 => Ok(Some(Cow::Owned(vec))),
            Ok(decoded_len) if len < decoded_len => {
                ctx.warnings.append_warning(Error::zlib_length_corrupted());
                Ok(None)
            }
            _ => {
                ctx.warnings.append_warning(Error::zlib_data_corrupted());
                Ok(None)
            }
        }
    }

    #[inline]
    pub fn uncompressed_length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        use byteorder::{ByteOrder, LittleEndian};
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        if input.is_empty() {
            return Ok(Some(0));
        }
        if input.len() <= 4 {
            ctx.warnings.append_warning(Error::zlib_data_corrupted());
            return Ok(Some(0));
        }
        Ok(Some(i64::from(LittleEndian::read_u32(&input[0..4]))))
    }

    pub fn random_bytes<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let len = try_opt!(self.children[0].eval_int(ctx, row));
        if len < 1 || len > 1024 {
            Err(Error::overflow("length", "random_bytes"))
        } else {
            Ok(Some(Cow::Owned(random_bytes(len as usize))))
        }
    }

    pub fn aes_encrypt<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let plaintext = try_opt!(self.children[0].eval_string(ctx, row));
        let key = try_opt!(self.children[1].eval_string(ctx, row));
        let iv = try_opt!(self.children[2].eval_string(ctx, row));
        let mode = try_opt!(self.children[3].eval_int(ctx, row)) as i32;

        if !NID.contains(&mode) {
            let nid = Nid::from_raw(mode);
            return Err(Error::unsupported_encryption_mode(
                &nid.long_name()
                    .map(|name| name.to_string())
                    .or_else::<(), _>(|_| Ok(format!("unknown OpenSSL Nid: {}", mode)))
                    .unwrap(),
            ));
        }

        let cipher = Cipher::from_nid(Nid::from_raw(mode)).unwrap();
        let key = aes_create_key(&key, cipher.key_len());
        let iv = check_iv(&iv, cipher)?;

        Ok(encrypt(cipher, &key, iv, &plaintext).ok().map(Cow::Owned))
    }

    pub fn aes_decrypt<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let plaintext = try_opt!(self.children[0].eval_string(ctx, row));
        let key = try_opt!(self.children[1].eval_string(ctx, row));
        let iv = try_opt!(self.children[2].eval_string(ctx, row));
        let mode = try_opt!(self.children[3].eval_int(ctx, row)) as i32;

        if !NID.contains(&mode) {
            let nid = Nid::from_raw(mode);
            return Err(Error::unsupported_encryption_mode(
                &nid.long_name()
                    .map(|name| name.to_string())
                    .or_else::<(), _>(|_| Ok(format!("unknown OpenSSL Nid: {}", mode)))
                    .unwrap(),
            ));
        }

        let cipher = Cipher::from_nid(Nid::from_raw(mode)).unwrap();
        let key = aes_create_key(&key, cipher.key_len());
        let iv = check_iv(&iv, cipher)?;

        Ok(decrypt(cipher, &key, iv, &plaintext).ok().map(Cow::Owned))
    }
}

#[cfg(test)]
mod tests {
    use super::super::Result;
    use super::random_bytes;
    use super::NID;

    use crate::coprocessor::codec::Datum;
    use crate::coprocessor::dag::expr::tests::{datum_expr, eval_func, scalar_func_expr};
    use crate::coprocessor::dag::expr::{EvalContext, Expression};
    use hex;
    use openssl;
    use tipb::expression::ScalarFuncSig;

    #[test]
    fn test_md5() {
        let cases = vec![
            ("", "d41d8cd98f00b204e9800998ecf8427e"),
            ("a", "0cc175b9c0f1b6a831c399e269772661"),
            ("ab", "187ef4436122d1cc2f40dc2b92f0eba0"),
            ("abc", "900150983cd24fb0d6963f7d28e17f72"),
            ("123", "202cb962ac59075b964b07152d234b70"),
        ];
        let mut ctx = EvalContext::default();

        for (input_str, exp_str) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::MD5, &[input]);
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp_str.as_bytes().to_vec());
            assert_eq!(got, exp, "md5('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::MD5, &[input]);
        let op = Expression::build(&ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "md5(NULL)");
    }

    #[test]
    fn test_sha1() {
        let cases = vec![
            ("", "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            ("a", "86f7e437faa5a7fce15d1ddcb9eaeaea377667b8"),
            ("ab", "da23614e02469a0d7c7bd1bdab5c9c474b1904dc"),
            ("abc", "a9993e364706816aba3e25717850c26c9cd0d89d"),
            ("123", "40bd001563085fc35165329ea1ff5c5ecbdbbeef"),
        ];
        let mut ctx = EvalContext::default();

        for (input_str, exp_str) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::SHA1, &[input]);
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp_str.as_bytes().to_vec());
            assert_eq!(got, exp, "sha1('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::SHA1, &[input]);
        let op = Expression::build(&ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "sha1(NULL)");
    }

    #[test]
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

        let mut ctx = EvalContext::default();

        for (input_str, hash_length_i64, exp_str) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let hash_length = datum_expr(Datum::I64(hash_length_i64));

            let op = scalar_func_expr(ScalarFuncSig::SHA2, &[input, hash_length]);
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp_str.as_bytes().to_vec());
            assert_eq!(got, exp, "sha2('{:?}', {:?})", input_str, hash_length_i64);
        }

        //test NULL case
        let null_cases = vec![
            (Datum::Null, Datum::I64(224), Datum::Null),
            (Datum::Bytes(b"pingcap".to_vec()), Datum::Null, Datum::Null),
            (
                Datum::Bytes(b"pingcap".to_vec()),
                Datum::I64(123),
                Datum::Null,
            ),
        ];

        for (input, hash_length, exp) in null_cases {
            let op = scalar_func_expr(
                ScalarFuncSig::SHA2,
                &[datum_expr(input.clone()), datum_expr(hash_length.clone())],
            );
            let op = Expression::build(&ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp, "sha2('{:?}', {:?})", input, hash_length);
        }
    }

    #[test]
    fn test_compress() {
        let cases = vec![
            (
                "hello world",
                "0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D",
            ),
            ("", ""),
            // compressed string ends with space
            (
                "hello wor012",
                "0C000000789CCB48CDC9C95728CF2F32303402001D8004202E",
            ),
        ];
        for (s, exp) in cases {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Compress, &[s]).unwrap();
            assert_eq!(
                got,
                Datum::Bytes(hex::decode(exp.as_bytes().to_vec()).unwrap())
            );
        }
    }

    #[test]
    fn test_uncompress() {
        let cases = vec![
            ("", Datum::Bytes(b"".to_vec())),
            (
                "0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D",
                Datum::Bytes(b"hello world".to_vec()),
            ),
            (
                "0C000000789CCB48CDC9C95728CF2F32303402001D8004202E",
                Datum::Bytes(b"hello wor012".to_vec()),
            ),
            // length is greater than the string
            (
                "12000000789CCB48CDC9C95728CF2FCA4901001A0B045D",
                Datum::Bytes(b"hello world".to_vec()),
            ),
            ("010203", Datum::Null),
            ("01020304", Datum::Null),
            ("020000000000", Datum::Null),
            // ZlibDecoder#read_to_end return 0
            ("0000000001", Datum::Null),
            // length is less than the string
            (
                "02000000789CCB48CDC9C95728CF2FCA4901001A0B045D",
                Datum::Null,
            ),
        ];
        for (s, exp) in cases {
            let s = Datum::Bytes(hex::decode(s.as_bytes().to_vec()).unwrap());
            let got = eval_func(ScalarFuncSig::Uncompress, &[s]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_uncompressed_length() {
        let cases = vec![
            ("", 0),
            ("0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D", 11),
            ("0C000000789CCB48CDC9C95728CF2F32303402001D8004202E", 12),
            ("020000000000", 2),
            ("0000000001", 0),
            ("02000000789CCB48CDC9C95728CF2FCA4901001A0B045D", 2),
            ("010203", 0),
            ("01020304", 0),
        ];
        for (s, exp) in cases {
            let s = Datum::Bytes(hex::decode(s.as_bytes().to_vec()).unwrap());
            let got = eval_func(ScalarFuncSig::UncompressedLength, &[s]).unwrap();
            assert_eq!(got, Datum::I64(exp));
        }
    }

    // AES block size is 16 bytes
    fn random_iv() -> Vec<u8> {
        random_bytes(16)
    }

    // Generate 16 random bytes as part of key
    fn random_key() -> Vec<u8> {
        random_bytes(16)
    }

    fn aes_encrypt(text: &[u8], key: &[u8], iv: &[u8], mode: i32) -> Result<Option<Vec<u8>>> {
        let text = Datum::from(text);
        let key = Datum::from(key);
        let iv = Datum::from(iv);
        let mode = Datum::from(i64::from(mode));

        eval_func(ScalarFuncSig::AesEncrypt, &[text, key, iv, mode]).map(|datum| match datum {
            Datum::Bytes(bytes) => Some(bytes),
            Datum::Null => None,
            _ => panic!("expected type `Vec<u8>/Null`, found others"),
        })
    }

    fn aes_decrypt(text: &[u8], key: &[u8], iv: &[u8], mode: i32) -> Result<Option<Vec<u8>>> {
        let text = Datum::from(text);
        let key = Datum::from(key);
        let iv = Datum::from(iv);
        let mode = Datum::from(i64::from(mode));

        eval_func(ScalarFuncSig::AesDecrypt, &[text, key, iv, mode]).map(|datum| match datum {
            Datum::Bytes(bytes) => Some(bytes),
            Datum::Null => None,
            _ => panic!("expected type `Vec<u8>/Null`, found others"),
        })
    }

    #[test]
    fn test_aes_encrypt_and_decrpyt() {
        openssl::init();
        let cases = vec![
            "How many roads must a man walk down before they call him a man?",
            "Hey Mister Tambourine Man, play a song for me",
            "La mer, qu'on voit danser le long des golfes clairs",
            "I can eat glass, it doesn't hurt me.",
        ];
        for &mode in NID.iter() {
            for &case in cases.iter() {
                let key = random_key();
                let iv = random_iv();
                let ciphertext = aes_encrypt(case.as_bytes(), &key, &iv, mode)
                    .unwrap()
                    .unwrap();
                let decrypted = aes_decrypt(&ciphertext, &key, &iv, mode).unwrap().unwrap();
                assert_eq!(case, String::from_utf8(decrypted).unwrap());
            }
        }

        // --------------------------------
        const AES_128_CTR: i32 = 904;
        const AES_128_CBC: i32 = 419;
        let key = random_key();
        let iv = random_iv();
        let plaintext = "too young, too simple, sometimes naive";
        // Fail because try to encrypt in an unsupported mode.
        assert!(aes_encrypt(plaintext.as_bytes(), &key, &iv, AES_128_CTR).is_err());
        // Fail because short IV.
        assert!(aes_encrypt(plaintext.as_bytes(), &key, &random_bytes(8), AES_128_CBC).is_err());

        for &mode in NID.iter() {
            let result = aes_decrypt(b"", &key, &iv, mode);
            // If the mode is block cipher mode(e.g cbc, ecb),
            // the recovered plaintext should be NULL because invalid ciphertext
            // Otherwise, it shold be empty string.
            match result {
                Ok(Some(bytes)) => assert!(bytes.is_empty()),
                Ok(None) => (),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_random_bytes() {
        let cases = vec![2, 4, 8, 16, 32];
        for len in cases {
            match eval_func(ScalarFuncSig::RandomBytes, &[Datum::from(len as i64)]) {
                Ok(Datum::Bytes(bytes)) => assert_eq!(len, bytes.len()),
                _ => panic!("expected type `Vec<u8>/Null`, found others"),
            }
        }

        // Invaild length for `random_bytes`
        let fail_cases = vec![0, 1025];
        for len in fail_cases {
            assert!(eval_func(ScalarFuncSig::RandomBytes, &[Datum::from(i64::from(len))]).is_err());
        }
    }
}

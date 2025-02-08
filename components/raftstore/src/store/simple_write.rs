// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::assert_matches::debug_assert_matches;

use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::{
    import_sstpb::SstMeta,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request},
};
use protobuf::{CodedInputStream, Message};
use slog::Logger;
use tikv_util::slog_panic;

use crate::store::{msg::ErrorCallback, WriteCallback};

// MAGIC number to hint simple write codec is used. If it's a protobuf message,
// the first one or several bytes are for field tag, which can't be zero.
// TODO: use protobuf blob request seems better.
const MAGIC_PREFIX: u8 = 0x00;

#[derive(Clone, Debug)]
pub struct SimpleWriteBinary {
    buf: Box<[u8]>,
    write_type: WriteType,
}

impl SimpleWriteBinary {
    /// Freeze the binary will forbid further batching.
    pub fn freeze(&mut self) {
        self.write_type = WriteType::Unspecified;
    }

    #[inline]
    pub fn data_size(&self) -> usize {
        self.buf.len()
    }
}

/// We usually use `RaftCmdRequest` for read write request. But the codec is
/// not efficient enough for simple request. `SimpleWrite` is introduce to
/// make codec alloc less and fast.
#[derive(Debug)]
pub struct SimpleWriteReqEncoder<C>
where
    C: ErrorCallback + WriteCallback,
{
    header: Box<RaftRequestHeader>,
    buf: Vec<u8>,
    channels: Vec<C>,
    size_limit: usize,
    write_type: WriteType,
}

impl<C> SimpleWriteReqEncoder<C>
where
    C: ErrorCallback + WriteCallback,
{
    /// Create a request encoder.
    pub fn new(
        header: Box<RaftRequestHeader>,
        bin: SimpleWriteBinary,
        size_limit: usize,
    ) -> SimpleWriteReqEncoder<C> {
        let mut buf = Vec::with_capacity(256);
        buf.push(MAGIC_PREFIX);
        header.write_length_delimited_to_vec(&mut buf).unwrap();
        buf.extend_from_slice(&bin.buf);

        SimpleWriteReqEncoder {
            header,
            buf,
            channels: vec![],
            size_limit,
            write_type: bin.write_type,
        }
    }

    /// Encode the simple write into the buffer.
    ///
    /// Return false if the buffer limit is reached or the binary type not
    /// match.
    #[inline]
    pub fn amend(&mut self, header: &RaftRequestHeader, bin: &SimpleWriteBinary) -> bool {
        if *self.header != *header {
            return false;
        }
        if self.write_type == bin.write_type
            && bin.write_type != WriteType::Unspecified
            && self.buf.len() + bin.buf.len() < self.size_limit
        {
            self.buf.extend_from_slice(&bin.buf);
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn data_size(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub fn encode(self) -> (Vec<u8>, Vec<C>) {
        (self.buf, self.channels)
    }

    #[inline]
    pub fn add_response_channel(&mut self, ch: C) {
        self.channels.push(ch);
    }

    #[inline]
    pub fn header(&self) -> &RaftRequestHeader {
        &self.header
    }
}

#[derive(Debug)]
pub struct Put<'a> {
    pub cf: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

#[derive(Debug)]
pub struct Delete<'a> {
    pub cf: &'a str,
    pub key: &'a [u8],
}

#[derive(Debug)]
pub struct DeleteRange<'a> {
    pub cf: &'a str,
    pub start_key: &'a [u8],
    pub end_key: &'a [u8],
    pub notify_only: bool,
}

#[derive(Debug)]
pub enum SimpleWrite<'a> {
    Put(Put<'a>),
    Delete(Delete<'a>),
    DeleteRange(DeleteRange<'a>),
    Ingest(Vec<SstMeta>),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum WriteType {
    Unspecified,
    PutDelete,
    DeleteRange,
    Ingest,
}

#[derive(Clone)]
pub struct SimpleWriteEncoder {
    buf: Vec<u8>,
    write_type: WriteType,
}

impl SimpleWriteEncoder {
    #[inline]
    pub fn with_capacity(cap: usize) -> SimpleWriteEncoder {
        SimpleWriteEncoder {
            buf: Vec::with_capacity(cap),
            write_type: WriteType::Unspecified,
        }
    }

    #[inline]
    pub fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) {
        debug_assert_matches!(
            self.write_type,
            WriteType::Unspecified | WriteType::PutDelete
        );
        encode(SimpleWrite::Put(Put { cf, key, value }), &mut self.buf);
        self.write_type = WriteType::PutDelete;
    }

    #[inline]
    pub fn delete(&mut self, cf: &str, key: &[u8]) {
        debug_assert_matches!(
            self.write_type,
            WriteType::Unspecified | WriteType::PutDelete
        );
        encode(SimpleWrite::Delete(Delete { cf, key }), &mut self.buf);
        self.write_type = WriteType::PutDelete;
    }

    #[inline]
    pub fn delete_range(&mut self, cf: &str, start_key: &[u8], end_key: &[u8], notify_only: bool) {
        debug_assert_matches!(
            self.write_type,
            WriteType::Unspecified | WriteType::DeleteRange
        );
        encode(
            SimpleWrite::DeleteRange(DeleteRange {
                cf,
                start_key,
                end_key,
                notify_only,
            }),
            &mut self.buf,
        );
        self.write_type = WriteType::DeleteRange;
    }

    #[inline]
    pub fn ingest(&mut self, sst: Vec<SstMeta>) {
        debug_assert_matches!(self.write_type, WriteType::Unspecified | WriteType::Ingest);
        encode(SimpleWrite::Ingest(sst), &mut self.buf);
        self.write_type = WriteType::Ingest;
    }

    #[inline]
    pub fn encode(self) -> SimpleWriteBinary {
        SimpleWriteBinary {
            buf: self.buf.into_boxed_slice(),
            write_type: self.write_type,
        }
    }
}

#[derive(Debug)]
pub struct SimpleWriteReqDecoder<'a> {
    header: RaftRequestHeader,
    buf: &'a [u8],
}

impl<'a> SimpleWriteReqDecoder<'a> {
    pub fn new(
        fallback: impl FnOnce(&'a [u8], u64, u64) -> RaftCmdRequest,
        logger: Option<&Logger>,
        buf: &'a [u8],
        index: u64,
        term: u64,
    ) -> Result<SimpleWriteReqDecoder<'a>, RaftCmdRequest> {
        match buf.first().cloned() {
            Some(MAGIC_PREFIX) => {
                let mut is = CodedInputStream::from_bytes(&buf[1..]);
                let header = match is.read_message() {
                    Ok(h) => h,
                    Err(e) => {
                        if let Some(logger) = logger {
                            slog_panic!(
                                logger,
                                "data corrupted";
                                "term" => term,
                                "index" => index,
                                "error" => ?e
                            )
                        } else {
                            panic!(
                                "data corrupted term: {}, index: {}, error {:?}",
                                term, index, e
                            )
                        }
                    }
                };
                let read = is.pos();
                Ok(SimpleWriteReqDecoder {
                    header,
                    buf: &buf[1 + read as usize..],
                })
            }
            _ => Err(fallback(buf, index, term)),
        }
    }

    #[inline]
    pub fn header(&self) -> &RaftRequestHeader {
        &self.header
    }

    pub fn to_raft_cmd_request(&self) -> RaftCmdRequest {
        let mut req = RaftCmdRequest::default();
        req.set_header(self.header().clone());
        let decoder = Self {
            header: Default::default(),
            buf: self.buf,
        };
        for s in decoder {
            match s {
                SimpleWrite::Put(Put { cf, key, value }) => {
                    let mut request = Request::default();
                    request.set_cmd_type(CmdType::Put);
                    request.mut_put().set_cf(cf.to_owned());
                    request.mut_put().set_key(key.to_owned());
                    request.mut_put().set_value(value.to_owned());
                    req.mut_requests().push(request);
                }
                SimpleWrite::Delete(Delete { cf, key }) => {
                    let mut request = Request::default();
                    request.set_cmd_type(CmdType::Delete);
                    request.mut_delete().set_cf(cf.to_owned());
                    request.mut_delete().set_key(key.to_owned());
                    req.mut_requests().push(request);
                }
                SimpleWrite::DeleteRange(DeleteRange {
                    cf,
                    start_key,
                    end_key,
                    notify_only,
                }) => {
                    let mut request = Request::default();
                    request.set_cmd_type(CmdType::DeleteRange);
                    request.mut_delete_range().set_cf(cf.to_owned());
                    request
                        .mut_delete_range()
                        .set_start_key(start_key.to_owned());
                    request.mut_delete_range().set_end_key(end_key.to_owned());
                    request.mut_delete_range().set_notify_only(notify_only);
                    req.mut_requests().push(request);
                }
                SimpleWrite::Ingest(ssts) => {
                    for sst in ssts {
                        let mut request = Request::default();
                        request.set_cmd_type(CmdType::IngestSst);
                        request.mut_ingest_sst().set_sst(sst);
                        req.mut_requests().push(request);
                    }
                }
            }
        }
        req
    }
}

impl<'a> Iterator for SimpleWriteReqDecoder<'a> {
    type Item = SimpleWrite<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        decode(&mut self.buf)
    }
}

const PUT_TAG: u8 = 0;
const DELETE_TAG: u8 = 1;
const DELETE_RANGE_TAG: u8 = 2;
const INGEST_TAG: u8 = 3;

const DEFAULT_CF_TAG: u8 = 0;
const WRITE_CF_TAG: u8 = 1;
const LOCK_CF_TAG: u8 = 2;
const ARBITRARY_CF_TAG: u8 = 3;

// Generally the length of most key is within 128. The length of value is
// within 2GiB.
// The algorithm can be checked in https://www.sqlite.org/src4/doc/trunk/www/varint.wiki.
#[inline]
fn encode_len(len: u32, buf: &mut Vec<u8>) {
    match len {
        0..=240 => buf.push(len as u8),
        241..=2287 => {
            buf.push((241 + (len - 240) / 256) as u8);
            buf.push(((len - 240) % 256) as u8);
        }
        2288..=67823 => {
            buf.push(249);
            buf.push(((len - 2288) / 256) as u8);
            buf.push(((len - 2288) % 256) as u8);
        }
        67824..=16777215 => {
            buf.push(250);
            let bytes = len.to_be_bytes();
            buf.extend_from_slice(&bytes[1..]);
        }
        16777216..=u32::MAX => {
            buf.push(251);
            let bytes = len.to_be_bytes();
            buf.extend_from_slice(&bytes);
        }
    }
}

#[inline]
fn decode_len(buf: &[u8]) -> (u32, &[u8]) {
    let (f, left) = buf.split_first().expect("decode len can't be 0");
    match f {
        0..=240 => (*f as u32, left),
        241..=248 => {
            let (s, left) = left.split_first().expect("decode len can't be 1");
            (240 + ((*f as u32) - 241) * 256 + *s as u32, left)
        }
        249 => {
            let (f, left) = left.split_at(2);
            (2288 + (f[0] as u32) * 256 + f[1] as u32, left)
        }
        250 => {
            let (f, left) = left.split_at(3);
            (u32::from_be_bytes([0, f[0], f[1], f[2]]), left)
        }
        251 => {
            let (f, left) = left.split_at(4);
            (u32::from_be_bytes([f[0], f[1], f[2], f[3]]), left)
        }
        _ => panic!("invalid len byte: {}", f),
    }
}

#[inline]
fn encode_bytes(bytes: &[u8], buf: &mut Vec<u8>) {
    encode_len(bytes.len() as u32, buf);
    buf.extend_from_slice(bytes);
}

#[inline]
fn decode_bytes(buf: &[u8]) -> (&[u8], &[u8]) {
    let (len, left) = decode_len(buf);
    left.split_at(len as usize)
}

#[inline]
fn encode_cf(cf: &str, buf: &mut Vec<u8>) {
    match cf {
        CF_DEFAULT => buf.push(DEFAULT_CF_TAG),
        CF_LOCK => buf.push(LOCK_CF_TAG),
        CF_WRITE => buf.push(WRITE_CF_TAG),
        cf => {
            // Perhaps should return error.
            buf.push(ARBITRARY_CF_TAG);
            encode_bytes(cf.as_bytes(), buf);
        }
    }
}

#[inline]
fn decode_cf(buf: &[u8]) -> (&str, &[u8]) {
    let (cf_tag, left) = buf.split_first().expect("cf cant't empty");
    match *cf_tag {
        DEFAULT_CF_TAG => (CF_DEFAULT, left),
        LOCK_CF_TAG => (CF_LOCK, left),
        WRITE_CF_TAG => (CF_WRITE, left),
        ARBITRARY_CF_TAG => {
            let (cf, left) = decode_bytes(left);
            (
                std::str::from_utf8(cf).expect("cf must be valid utf8"),
                left,
            )
        }
        _ => panic!("invalid cf tag: {}", cf_tag),
    }
}

#[inline(always)]
fn encode(simple_write: SimpleWrite<'_>, buf: &mut Vec<u8>) {
    match simple_write {
        SimpleWrite::Put(put) => {
            buf.push(PUT_TAG);
            encode_cf(put.cf, buf);
            encode_bytes(put.key, buf);
            encode_bytes(put.value, buf);
        }
        SimpleWrite::Delete(delete) => {
            buf.push(DELETE_TAG);
            encode_cf(delete.cf, buf);
            encode_bytes(delete.key, buf);
        }
        SimpleWrite::DeleteRange(dr) => {
            buf.push(DELETE_RANGE_TAG);
            encode_cf(dr.cf, buf);
            encode_bytes(dr.start_key, buf);
            encode_bytes(dr.end_key, buf);
            buf.push(dr.notify_only as u8);
        }
        SimpleWrite::Ingest(ssts) => {
            buf.push(INGEST_TAG);
            encode_len(ssts.len() as u32, buf);
            // IngestSST is not a frequent operation, use protobuf to reduce complexity.
            for sst in ssts {
                sst.write_length_delimited_to_vec(buf).unwrap();
            }
        }
    }
}

#[inline]
fn decode<'a>(buf: &mut &'a [u8]) -> Option<SimpleWrite<'a>> {
    let (tag, left) = buf.split_first()?;
    match *tag {
        PUT_TAG => {
            let (cf, left) = decode_cf(left);
            let (key, left) = decode_bytes(left);
            let (value, left) = decode_bytes(left);
            *buf = left;
            Some(SimpleWrite::Put(Put { cf, key, value }))
        }
        DELETE_TAG => {
            let (cf, left) = decode_cf(left);
            let (key, left) = decode_bytes(left);
            *buf = left;
            Some(SimpleWrite::Delete(Delete { cf, key }))
        }
        DELETE_RANGE_TAG => {
            let (cf, left) = decode_cf(left);
            let (start_key, left) = decode_bytes(left);
            let (end_key, left) = decode_bytes(left);
            let (notify_only, left) = left.split_first()?;
            *buf = left;
            Some(SimpleWrite::DeleteRange(DeleteRange {
                cf,
                start_key,
                end_key,
                notify_only: *notify_only != 0,
            }))
        }
        INGEST_TAG => {
            let (len, left) = decode_len(left);
            let mut ssts = Vec::with_capacity(len as usize);
            let mut is = CodedInputStream::from_bytes(left);
            for _ in 0..len {
                let sst = match is.read_message() {
                    Ok(sst) => sst,
                    Err(e) => panic!("data corrupted {:?}", e),
                };
                ssts.push(sst);
            }
            let read = is.pos();
            *buf = &left[read as usize..];
            Some(SimpleWrite::Ingest(ssts))
        }
        tag => panic!("corrupted data: invalid tag {}", tag),
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use kvproto::raft_cmdpb::{CmdType, Request};
    use slog::o;

    use super::*;
    use crate::store::Callback;

    fn decoder_fallback(data: &[u8], index: u64, _: u64) -> RaftCmdRequest {
        crate::store::util::parse_data_at(data, index, "")
    }

    #[test]
    fn test_codec() {
        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.put(CF_DEFAULT, b"key", b"");
        let delete_key = vec![0; 1024];
        encoder.delete(CF_WRITE, &delete_key);
        let bin = encoder.encode();

        let mut header = Box::<RaftRequestHeader>::default();
        header.set_term(2);
        let mut req_encoder = SimpleWriteReqEncoder::<Callback<engine_rocks::RocksSnapshot>>::new(
            header.clone(),
            bin,
            usize::MAX,
        );

        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.delete_range(CF_LOCK, b"key", b"key", true);
        encoder.delete_range("cf", b"key", b"key", false);
        let bin = encoder.encode();
        assert!(!req_encoder.amend(&header, &bin));
        let req_encoder2 = SimpleWriteReqEncoder::<Callback<engine_rocks::RocksSnapshot>>::new(
            header.clone(),
            bin,
            0,
        );

        let (bytes, _) = req_encoder.encode();
        let logger = slog_global::borrow_global().new(o!());
        let mut decoder =
            SimpleWriteReqDecoder::new(decoder_fallback, Some(&logger), &bytes, 0, 0).unwrap();
        assert_eq!(*decoder.header(), *header);
        let write = decoder.next().unwrap();
        let SimpleWrite::Put(put) = write else {
            panic!("should be put")
        };
        assert_eq!(put.cf, CF_DEFAULT);
        assert_eq!(put.key, b"key");
        assert_eq!(put.value, b"");

        let write = decoder.next().unwrap();
        let SimpleWrite::Delete(delete) = write else {
            panic!("should be delete")
        };
        assert_eq!(delete.cf, CF_WRITE);
        assert_eq!(delete.key, &delete_key);
        assert_matches!(decoder.next(), None);

        let (bytes, _) = req_encoder2.encode();
        decoder =
            SimpleWriteReqDecoder::new(decoder_fallback, Some(&logger), &bytes, 0, 0).unwrap();
        let write = decoder.next().unwrap();
        let SimpleWrite::DeleteRange(dr) = write else {
            panic!("should be delete range")
        };
        assert_eq!(dr.cf, CF_LOCK);
        assert_eq!(dr.start_key, b"key");
        assert_eq!(dr.end_key, b"key");
        assert!(dr.notify_only);

        let write = decoder.next().unwrap();
        let SimpleWrite::DeleteRange(dr) = write else {
            panic!("should be delete range")
        };
        assert_eq!(dr.cf, "cf");
        assert_eq!(dr.start_key, b"key");
        assert_eq!(dr.end_key, b"key");
        assert!(!dr.notify_only);

        let res = decoder.next();
        assert!(res.is_none(), "{:?}", res);

        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        let exp: Vec<_> = (0..10)
            .map(|id| {
                let mut meta = SstMeta::default();
                meta.set_region_id(id);
                meta
            })
            .collect();
        encoder.ingest(exp.clone());
        let bin = encoder.encode();
        let req_encoder =
            SimpleWriteReqEncoder::<Callback<engine_rocks::RocksSnapshot>>::new(header, bin, 0);
        let (bytes, _) = req_encoder.encode();
        let mut decoder =
            SimpleWriteReqDecoder::new(decoder_fallback, Some(&logger), &bytes, 0, 0).unwrap();
        let write = decoder.next().unwrap();
        let SimpleWrite::Ingest(ssts) = write else {
            panic!("should be ingest")
        };
        assert_eq!(exp, ssts);
        assert_matches!(decoder.next(), None);
    }

    #[test]
    fn test_encode_num() {
        let mut buf = Vec::new();
        let cases = vec![
            0,
            1,
            240,
            241,
            2287,
            2288,
            67823,
            67824,
            16777215,
            16777216,
            u32::MAX,
        ];
        for n in cases {
            super::encode_len(n, &mut buf);
            buf.push(0);
            let (m, left) = super::decode_len(&buf);
            assert_eq!(n, m);
            assert_eq!(left, &[0]);
            buf.clear();
        }
    }

    #[test]
    fn test_invalid() {
        let mut raft_cmd = RaftCmdRequest::default();
        raft_cmd.mut_header().set_term(2);

        let mut req = Request::default();
        req.set_cmd_type(CmdType::Invalid);
        raft_cmd.mut_requests().push(req);
        let bytes = raft_cmd.write_to_bytes().unwrap();
        let logger = slog_global::borrow_global().new(o!());
        let decoded =
            SimpleWriteReqDecoder::new(decoder_fallback, Some(&logger), &bytes, 0, 0).unwrap_err();
        // SimpleWriteReqDecoder should be able to decode naive RaftCmdRequest.
        assert_eq!(decoded, raft_cmd);

        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.put(CF_DEFAULT, b"key", b"");
        let bin = encoder.encode();

        let mut header = Box::<RaftRequestHeader>::default();
        header.set_term(2);
        let mut req_encoder: SimpleWriteReqEncoder<Callback<engine_rocks::RocksSnapshot>> =
            SimpleWriteReqEncoder::<Callback<engine_rocks::RocksSnapshot>>::new(
                header.clone(),
                bin.clone(),
                512,
            );

        let mut header2 = Box::<RaftRequestHeader>::default();
        header2.set_term(4);
        // Only simple write command with same header can be batched.
        assert!(!req_encoder.amend(&header2, &bin));

        let mut bin2 = bin.clone();
        bin2.freeze();
        // Frozen bin can't be merged with other bin.
        assert!(!req_encoder.amend(&header, &bin2));
        let mut req_encoder2: SimpleWriteReqEncoder<Callback<engine_rocks::RocksSnapshot>> =
            SimpleWriteReqEncoder::<Callback<engine_rocks::RocksSnapshot>>::new(
                header.clone(),
                bin2.clone(),
                512,
            );
        assert!(!req_encoder2.amend(&header, &bin));

        // Batch should not excceed max size limit.
        let large_value = vec![0; 512];
        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.put(CF_DEFAULT, b"key", &large_value);
        assert!(!req_encoder.amend(&header, &encoder.encode()));

        let (bytes, _) = req_encoder.encode();
        let mut decoder =
            SimpleWriteReqDecoder::new(decoder_fallback, Some(&logger), &bytes, 0, 0).unwrap();
        assert_eq!(*decoder.header(), *header);
        let req = decoder.next().unwrap();
        let SimpleWrite::Put(put) = req else {
            panic!("should be put")
        };
        assert_eq!(put.cf, CF_DEFAULT);
        assert_eq!(put.key, b"key");
        assert_eq!(put.value, b"");

        let res = decoder.next();
        assert!(res.is_none(), "{:?}", res);
    }

    #[test]
    fn test_to_raft_cmd_request() {
        let logger = slog_global::borrow_global().new(o!());

        // Test header.
        let mut header = Box::<RaftRequestHeader>::default();
        header.set_term(2);
        let req_encoder = SimpleWriteReqEncoder::<Callback<engine_rocks::RocksSnapshot>>::new(
            header.clone(),
            SimpleWriteEncoder::with_capacity(512).encode(),
            512,
        );
        let (bin, _) = req_encoder.encode();
        assert_eq!(
            header.as_ref(),
            SimpleWriteReqDecoder::new(decoder_fallback, Some(&logger), &bin, 0, 0)
                .unwrap()
                .to_raft_cmd_request()
                .get_header(),
        );

        // Test put.
        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.put(CF_WRITE, b"write", b"value");
        let req_encoder = SimpleWriteReqEncoder::<Callback<engine_rocks::RocksSnapshot>>::new(
            header.clone(),
            encoder.encode(),
            512,
        );
        let (bin, _) = req_encoder.encode();
        let req = SimpleWriteReqDecoder::new(decoder_fallback, Some(&logger), &bin, 0, 0)
            .unwrap()
            .to_raft_cmd_request();
        assert_eq!(req.get_requests().len(), 1);
        assert_eq!(req.get_requests()[0].get_put().get_cf(), CF_WRITE);
        assert_eq!(req.get_requests()[0].get_put().get_key(), b"write");
        assert_eq!(req.get_requests()[0].get_put().get_value(), b"value");

        // Test delete.
        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.delete(CF_DEFAULT, b"write");
        let req_encoder = SimpleWriteReqEncoder::<Callback<engine_rocks::RocksSnapshot>>::new(
            header.clone(),
            encoder.encode(),
            512,
        );
        let (bin, _) = req_encoder.encode();
        let req = SimpleWriteReqDecoder::new(decoder_fallback, Some(&logger), &bin, 0, 0)
            .unwrap()
            .to_raft_cmd_request();
        assert_eq!(req.get_requests().len(), 1);
        assert_eq!(req.get_requests()[0].get_delete().get_cf(), CF_DEFAULT);
        assert_eq!(req.get_requests()[0].get_delete().get_key(), b"write");

        // Test delete range.
        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.delete_range(CF_LOCK, b"start", b"end", true);
        let req_encoder = SimpleWriteReqEncoder::<Callback<engine_rocks::RocksSnapshot>>::new(
            header.clone(),
            encoder.encode(),
            512,
        );
        let (bin, _) = req_encoder.encode();
        let req = SimpleWriteReqDecoder::new(decoder_fallback, Some(&logger), &bin, 0, 0)
            .unwrap()
            .to_raft_cmd_request();
        assert_eq!(req.get_requests().len(), 1);
        assert_eq!(req.get_requests()[0].get_delete_range().get_cf(), CF_LOCK);
        assert_eq!(
            req.get_requests()[0].get_delete_range().get_start_key(),
            b"start"
        );
        assert_eq!(
            req.get_requests()[0].get_delete_range().get_end_key(),
            b"end"
        );
        assert_eq!(
            req.get_requests()[0].get_delete_range().get_notify_only(),
            true
        );

        // Test ingest.
        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.ingest(vec![SstMeta::default(); 5]);
        let req_encoder = SimpleWriteReqEncoder::<Callback<engine_rocks::RocksSnapshot>>::new(
            header,
            encoder.encode(),
            512,
        );
        let (bin, _) = req_encoder.encode();
        let req = SimpleWriteReqDecoder::new(decoder_fallback, Some(&logger), &bin, 0, 0)
            .unwrap()
            .to_raft_cmd_request();
        assert_eq!(req.get_requests().len(), 5);
        assert!(req.get_requests()[0].has_ingest_sst());
        assert!(req.get_requests()[4].has_ingest_sst());
    }
}

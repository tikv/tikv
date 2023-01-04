// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftRequestHeader};
use protobuf::{CodedInputStream, Message};
use raftstore::store::WriteCallback;
use slog::Logger;

use crate::{operation::command::parse_at, router::CmdResChannel};

// MAGIC number to hint simple write codec is used. If it's a protobuf message,
// the first one or several bytes are for field tag, which can't be zero.
// TODO: use protobuf blob request seems better.
const MAGIC_PREFIX: u8 = 0x00;

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct SimpleWriteBinary {
    buf: Box<[u8]>,
}

/// We usually use `RaftCmdRequest` for read write request. But the codec is
/// not efficient enough for simple request. `SimpleWrite` is introduce to make
/// codec alloc less and fast.
#[derive(Debug)]
pub struct SimpleWriteReqEncoder {
    header: Box<RaftRequestHeader>,
    buf: Vec<u8>,
    channels: Vec<CmdResChannel>,
    size_limit: usize,
    notify_proposed: bool,
}

impl SimpleWriteReqEncoder {
    /// Create a request encoder.
    ///
    /// If `notify_proposed` is true, channels will be called `notify_proposed`
    /// when it's appended.
    pub fn new(
        header: Box<RaftRequestHeader>,
        bin: SimpleWriteBinary,
        size_limit: usize,
        notify_proposed: bool,
    ) -> SimpleWriteReqEncoder {
        let mut buf = Vec::with_capacity(256);
        buf.push(MAGIC_PREFIX);
        header.write_length_delimited_to_vec(&mut buf).unwrap();
        buf.extend_from_slice(&bin.buf);

        SimpleWriteReqEncoder {
            header,
            buf,
            channels: vec![],
            size_limit,
            notify_proposed,
        }
    }

    /// Encode the simple write into the buffer dispite header check.
    ///
    /// Return false if the buffer limit is reached or the write can be amended.
    #[inline]
    pub fn amend(&mut self, header: &RaftRequestHeader, bin: &SimpleWriteBinary) -> bool {
        if *self.header != *header {
            return false;
        }
        if self.buf.len() + bin.buf.len() < self.size_limit {
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
    pub fn encode(self) -> (Vec<u8>, Vec<CmdResChannel>) {
        (self.buf, self.channels)
    }

    #[inline]
    pub fn add_response_channel(&mut self, mut ch: CmdResChannel) {
        if self.notify_proposed {
            ch.notify_proposed();
        }
        self.channels.push(ch);
    }

    #[inline]
    pub fn notify_proposed(&self) -> bool {
        self.notify_proposed
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
}

#[derive(Clone)]
pub struct SimpleWriteEncoder {
    buf: Vec<u8>,
}

impl SimpleWriteEncoder {
    #[inline]
    pub fn with_capacity(cap: usize) -> SimpleWriteEncoder {
        SimpleWriteEncoder {
            buf: Vec::with_capacity(cap),
        }
    }

    #[inline]
    pub fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) {
        encode(SimpleWrite::Put(Put { cf, key, value }), &mut self.buf);
    }

    #[inline]
    pub fn delete(&mut self, cf: &str, key: &[u8]) {
        encode(SimpleWrite::Delete(Delete { cf, key }), &mut self.buf);
    }

    #[inline]
    pub fn delete_range(&mut self, cf: &str, start_key: &[u8], end_key: &[u8], notify_only: bool) {
        encode(
            SimpleWrite::DeleteRange(DeleteRange {
                cf,
                start_key,
                end_key,
                notify_only,
            }),
            &mut self.buf,
        );
    }

    #[inline]
    pub fn encode(self) -> SimpleWriteBinary {
        SimpleWriteBinary {
            buf: self.buf.into_boxed_slice(),
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
        logger: &Logger,
        buf: &'a [u8],
        index: u64,
        term: u64,
    ) -> Result<SimpleWriteReqDecoder<'a>, RaftCmdRequest> {
        match buf.first().cloned() {
            Some(MAGIC_PREFIX) => {
                let mut is = CodedInputStream::from_bytes(&buf[1..]);
                let header = match is.read_message() {
                    Ok(h) => h,
                    Err(e) => panic!(
                        "{:?} data corrupted at [{}] {}: {:?}",
                        logger.list(),
                        term,
                        index,
                        e
                    ),
                };
                let read = is.pos();
                Ok(SimpleWriteReqDecoder {
                    header,
                    buf: &buf[1 + read as usize..],
                })
            }
            _ => Err(parse_at(logger, buf, index, term)),
        }
    }

    #[inline]
    pub fn header(&self) -> &RaftRequestHeader {
        &self.header
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
        tag => panic!("corrupted data: invalid tag {}", tag),
    }
}

#[cfg(test)]
mod tests {
    use kvproto::raft_cmdpb::{CmdType, Request};
    use slog::o;

    use super::*;

    #[test]
    fn test_codec() {
        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.put(CF_DEFAULT, b"key", b"");
        let delete_key = vec![0; 1024];
        encoder.delete(CF_WRITE, &delete_key);
        let bin = encoder.encode();

        let mut header = Box::<RaftRequestHeader>::default();
        header.set_term(2);
        let mut req_encoder = SimpleWriteReqEncoder::new(header.clone(), bin, usize::MAX, false);

        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.delete_range(CF_LOCK, b"key", b"key", true);
        encoder.delete_range("cf", b"key", b"key", false);
        req_encoder.amend(&header, &encoder.encode());

        let (bytes, _) = req_encoder.encode();
        let logger = slog_global::borrow_global().new(o!());
        let mut decoder = SimpleWriteReqDecoder::new(&logger, &bytes, 0, 0).unwrap();
        assert_eq!(*decoder.header(), *header);
        let write = decoder.next().unwrap();
        let SimpleWrite::Put(put) = write else { panic!("should be put") };
        assert_eq!(put.cf, CF_DEFAULT);
        assert_eq!(put.key, b"key");
        assert_eq!(put.value, b"");

        let write = decoder.next().unwrap();
        let SimpleWrite::Delete(delete) = write else { panic!("should be delete") };
        assert_eq!(delete.cf, CF_WRITE);
        assert_eq!(delete.key, &delete_key);

        let write = decoder.next().unwrap();
        let SimpleWrite::DeleteRange(dr) = write else { panic!("should be delete range") };
        assert_eq!(dr.cf, CF_LOCK);
        assert_eq!(dr.start_key, b"key");
        assert_eq!(dr.end_key, b"key");
        assert!(dr.notify_only);

        let write = decoder.next().unwrap();
        let SimpleWrite::DeleteRange(dr) = write else { panic!("should be delete range") };
        assert_eq!(dr.cf, "cf");
        assert_eq!(dr.start_key, b"key");
        assert_eq!(dr.end_key, b"key");
        assert!(!dr.notify_only);

        let res = decoder.next();
        assert!(res.is_none(), "{:?}", res);
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
        let decoded = SimpleWriteReqDecoder::new(&logger, &bytes, 0, 0).unwrap_err();
        // SimpleWriteReqDecoder should be able to decode naive RaftCmdRequest.
        assert_eq!(decoded, raft_cmd);

        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.put(CF_DEFAULT, b"key", b"");
        let bin = encoder.encode();

        let mut header = Box::<RaftRequestHeader>::default();
        header.set_term(2);
        let mut req_encoder = SimpleWriteReqEncoder::new(header.clone(), bin.clone(), 512, false);

        let mut header2 = Box::<RaftRequestHeader>::default();
        header2.set_term(4);
        // Only simple write command with same header can be batched.
        assert!(!req_encoder.amend(&header2, &bin));

        // Batch should not excceed max size limit.
        let large_value = vec![0; 512];
        let mut encoder = SimpleWriteEncoder::with_capacity(512);
        encoder.put(CF_DEFAULT, b"key", &large_value);
        assert!(!req_encoder.amend(&header, &encoder.encode()));

        let (bytes, _) = req_encoder.encode();
        let mut decoder = SimpleWriteReqDecoder::new(&logger, &bytes, 0, 0).unwrap();
        assert_eq!(*decoder.header(), *header);
        let req = decoder.next().unwrap();
        let SimpleWrite::Put(put) = req else { panic!("should be put") };
        assert_eq!(put.cf, CF_DEFAULT);
        assert_eq!(put.key, b"key");
        assert_eq!(put.value, b"");

        let res = decoder.next();
        assert!(res.is_none(), "{:?}", res);
    }
}

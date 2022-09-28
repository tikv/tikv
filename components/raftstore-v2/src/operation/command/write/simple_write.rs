// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;

use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request};
use protobuf::{CodedInputStream, Message, SingularPtrField};
use tikv_util::Either;

use crate::router::CmdResChannel;

// MAGIC number to hint simple write codec is used. If it's a protobuf message,
// the first one or several bytes are for field tag, which can't be zero.
// TODO: use protobuf blob request seems better.
const MAGIC_PREFIX: u8 = 0x00;

/// We usually use `RaftCmdRequest` for read write request. But the codec is
/// not efficient enough for simple request. `SimpleWrite` is introduce to make
/// codec alloc less and fast.
#[derive(Debug)]
pub struct SimpleWriteEncoder {
    header: SingularPtrField<RaftRequestHeader>,
    buf: Vec<u8>,
    channels: Vec<CmdResChannel>,
    size_limit: usize,
}

impl SimpleWriteEncoder {
    pub fn new(
        mut req: RaftCmdRequest,
        size_limit: usize,
    ) -> Result<SimpleWriteEncoder, RaftCmdRequest> {
        if !Self::allow_request(&req) {
            return Err(req);
        }

        let mut buf = Vec::with_capacity(256);
        buf.push(MAGIC_PREFIX);
        req.get_header()
            .write_length_delimited_to_vec(&mut buf)
            .unwrap();

        for r in req.get_requests() {
            encode(r, &mut buf);
        }
        Ok(SimpleWriteEncoder {
            header: req.header,
            buf,
            channels: vec![],
            size_limit,
        })
    }

    fn allow_request(req: &RaftCmdRequest) -> bool {
        if !req.has_status_request() && !req.has_admin_request() {
            // TODO: skip the check and make caller use `SimpleWrite` directly.
            for r in req.get_requests() {
                if r.get_cmd_type() != CmdType::Put
                    && r.get_cmd_type() != CmdType::Delete
                    && r.get_cmd_type() != CmdType::DeleteRange
                {
                    return false;
                }
            }
        } else {
            return false;
        };
        true
    }

    #[inline]
    pub fn amend(&mut self, req: RaftCmdRequest) -> Result<(), RaftCmdRequest> {
        if Self::allow_request(&req) && req.header == self.header {
            let last_length = self.buf.len();
            for r in req.get_requests() {
                encode(r, &mut self.buf);
            }
            // The default size limit is 8 * 0.4 = 3.2MiB.
            if self.buf.len() < self.size_limit {
                Ok(())
            } else {
                self.buf.truncate(last_length);
                Err(req)
            }
        } else {
            Err(req)
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
    pub fn add_response_channel(&mut self, ch: CmdResChannel) {
        self.channels.push(ch);
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

#[derive(Debug)]
pub struct SimpleWriteDecoder<'a> {
    header: RaftRequestHeader,
    buf: &'a [u8],
}

impl<'a> SimpleWriteDecoder<'a> {
    pub fn new(buf: &'a [u8]) -> Result<SimpleWriteDecoder<'a>, RaftCmdRequest> {
        match buf.first().cloned() {
            Some(MAGIC_PREFIX) => {
                let mut is = CodedInputStream::from_bytes(&buf[1..]);
                let header = is.read_message().unwrap();
                let read = is.pos();
                Ok(SimpleWriteDecoder {
                    header,
                    buf: &buf[1 + read as usize..],
                })
            }
            _ => {
                let mut req = RaftCmdRequest::new();
                req.merge_from_bytes(buf).unwrap();
                Err(req)
            }
        }
    }

    #[inline]
    pub fn header(&self) -> &RaftRequestHeader {
        &self.header
    }
}

impl<'a> Iterator for SimpleWriteDecoder<'a> {
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

// TODO: we need a way to verify every field is encoded.
#[inline]
fn encode(req: &Request, buf: &mut Vec<u8>) {
    match req.get_cmd_type() {
        CmdType::Put => {
            buf.push(PUT_TAG);
            let put_req = req.get_put();
            encode_cf(put_req.get_cf(), buf);
            encode_bytes(put_req.get_key(), buf);
            encode_bytes(put_req.get_value(), buf);
        }
        CmdType::Delete => {
            buf.push(DELETE_TAG);
            let delete_req = req.get_delete();
            encode_cf(delete_req.get_cf(), buf);
            encode_bytes(delete_req.get_key(), buf);
        }
        CmdType::DeleteRange => {
            buf.push(DELETE_RANGE_TAG);
            let delete_range_req = req.get_delete_range();
            encode_cf(delete_range_req.get_cf(), buf);
            encode_bytes(delete_range_req.get_start_key(), buf);
            encode_bytes(delete_range_req.get_end_key(), buf);
            buf.push(delete_range_req.get_notify_only() as u8);
        }
        CmdType::Invalid
        | CmdType::Get
        | CmdType::Snap
        | CmdType::Prewrite
        | CmdType::IngestSst
        | CmdType::ReadIndex => unreachable!("not supported type should be filtered already"),
    }
}

#[inline]
fn decode<'a>(buf: &mut &'a [u8]) -> Option<SimpleWrite<'a>> {
    let (tag, mut left) = buf.split_first()?;
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
    use super::*;

    #[test]
    fn test_codec() {
        let mut cmd = RaftCmdRequest::default();
        cmd.mut_header().set_term(2);

        let mut req = Request::default();
        req.set_cmd_type(CmdType::Put);
        let put_req = req.mut_put();
        put_req.set_cf(CF_DEFAULT.to_string());
        put_req.set_key(b"key".to_vec());
        put_req.set_value(b"".to_vec());
        cmd.mut_requests().push(req);

        req = Request::default();
        req.set_cmd_type(CmdType::Delete);
        let delete_req = req.mut_delete();
        let delete_key = vec![0; 1024];
        delete_req.set_cf(CF_WRITE.to_string());
        delete_req.set_key(delete_key.clone());
        cmd.mut_requests().push(req);

        let mut encoder = SimpleWriteEncoder::new(cmd.clone(), usize::MAX).unwrap();
        cmd.clear_requests();

        req = Request::default();
        req.set_cmd_type(CmdType::DeleteRange);
        let delete_range_req = req.mut_delete_range();
        delete_range_req.set_cf(CF_LOCK.to_string());
        delete_range_req.set_start_key(b"key".to_vec());
        delete_range_req.set_end_key(b"key".to_vec());
        delete_range_req.set_notify_only(true);
        cmd.mut_requests().push(req);

        req = Request::default();
        req.set_cmd_type(CmdType::DeleteRange);
        let delete_range_req = req.mut_delete_range();
        delete_range_req.set_cf("cf".to_string());
        delete_range_req.set_start_key(b"key".to_vec());
        delete_range_req.set_end_key(b"key".to_vec());
        delete_range_req.set_notify_only(false);
        cmd.mut_requests().push(req);

        encoder.amend(cmd.clone()).unwrap();
        let (bytes, _) = encoder.encode();
        let mut decoder = SimpleWriteDecoder::new(&bytes).unwrap();
        assert_eq!(decoder.header(), cmd.get_header());
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
        let mut invalid_cmd = RaftCmdRequest::default();
        invalid_cmd.mut_header().set_term(2);

        let mut req = Request::default();
        req.set_cmd_type(CmdType::Invalid);
        invalid_cmd.mut_requests().push(req);
        let fallback = SimpleWriteEncoder::new(invalid_cmd.clone(), usize::MAX).unwrap_err();
        let bytes = fallback.write_to_bytes().unwrap();
        let decoded = SimpleWriteDecoder::new(&bytes).unwrap_err();
        assert_eq!(decoded, invalid_cmd);

        let mut valid_cmd = RaftCmdRequest::default();
        valid_cmd.mut_header().set_term(3);
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Put);
        let put_req = req.mut_put();
        put_req.set_cf(CF_DEFAULT.to_string());
        put_req.set_key(b"key".to_vec());
        put_req.set_value(b"".to_vec());
        valid_cmd.mut_requests().push(req);
        let mut encoder = SimpleWriteEncoder::new(valid_cmd.clone(), usize::MAX).unwrap();
        // Only simple write command can be batched.
        encoder.amend(invalid_cmd.clone()).unwrap_err();
        let mut valid_cmd2 = valid_cmd.clone();
        valid_cmd2.mut_header().set_term(4);
        // Only simple write command with same header can be batched.
        encoder.amend(valid_cmd2).unwrap_err();

        let (bytes, _) = encoder.encode();
        let mut decoder = SimpleWriteDecoder::new(&bytes).unwrap();
        assert_eq!(decoder.header(), valid_cmd.get_header());
        let req = decoder.next().unwrap();
        let SimpleWrite::Put(put) = req else { panic!("should be put") };
        assert_eq!(put.cf, CF_DEFAULT);
        assert_eq!(put.key, b"key");
        assert_eq!(put.value, b"");

        let res = decoder.next();
        assert!(res.is_none(), "{:?}", res);
    }
}

// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// This package handles RPC message data encoding/decoding.
// Every RPC message data contains two parts: header + payload.
// Header is 16 bytes, format:
//  | 0xdaf4(2 bytes magic value) | 0x01(version 2 bytes) | msg_len(4 bytes) | msg_id(8 bytes) |,
// all use bigendian.
// Now the version is always 1.
// Payload can be any arbitrary data, but we use Protobuf in our program default.
use std::io::{self, BufRead};
use std::vec::Vec;

use byteorder::{ByteOrder, BigEndian, ReadBytesExt};
use protobuf::{self, CodedInputStream};

use super::{Result, Error};

pub const MSG_HEADER_LEN: usize = 16;
pub const MSG_MAGIC: u16 = 0xdaf4;
pub const MSG_VERSION_V1: u16 = 1;


fn other_err(msg: String) -> Error {
    Error::Io(io::Error::new(io::ErrorKind::Other, msg))
}


// Encodes message with message ID and protobuf body.
pub fn encode_msg<T: io::Write, M: protobuf::Message + ?Sized>(w: &mut T,
                                                               msg_id: u64,
                                                               msg: &M)
                                                               -> Result<()> {
    let payload_len = msg.compute_size();
    let header = encode_msg_header(msg_id, payload_len as usize);
    try!(w.write_all(&header));
    try!(msg.write_to_writer(w));

    Ok(())
}

// Decodes encoded message, returns message ID.
pub fn decode_msg<T: io::Read, M: protobuf::Message>(r: &mut T, m: &mut M) -> Result<u64> {
    let (message_id, payload_len) = try!(decode_msg_header(r));
    let mut payload = vec![0; payload_len];
    try!(r.read_exact(&mut payload));
    let mut reader = payload.as_slice();
    try!(decode_body(&mut reader, m));

    Ok(message_id)
}

// Encodes msg header to a 16 bytes header buffer.
pub fn encode_msg_header(msg_id: u64, payload_len: usize) -> Vec<u8> {
    let mut buf = vec![0;MSG_HEADER_LEN];

    BigEndian::write_u16(&mut buf[0..2], MSG_MAGIC);
    BigEndian::write_u16(&mut buf[2..4], MSG_VERSION_V1);
    BigEndian::write_u32(&mut buf[4..8], payload_len as u32);
    BigEndian::write_u64(&mut buf[8..16], msg_id);

    buf
}

// Decodes msg header in header buffer, the buffer length size must be equal MSG_HEADER_LEN;
pub fn decode_msg_header<R: io::Read>(header: &mut R) -> Result<(u64, usize)> {
    let magic = try!(header.read_u16::<BigEndian>());
    if MSG_MAGIC != magic {
        return Err(other_err(format!("invalid magic {}, not {}", magic, MSG_MAGIC)));
    }

    let version = try!(header.read_u16::<BigEndian>());
    if MSG_VERSION_V1 != version {
        return Err(other_err(format!("unsupported version {}, we need {} now",
                                     version,
                                     MSG_VERSION_V1)));
    }

    let payload_len = try!(header.read_u32::<BigEndian>()) as usize;
    // TODO: check max payload

    let message_id = try!(header.read_u64::<BigEndian>());

    Ok((message_id, payload_len))
}

// Decodes only body.
pub fn decode_body<R: BufRead, M: protobuf::Message>(payload: &mut R, m: &mut M) -> Result<()> {
    let mut is = CodedInputStream::from_buffered_reader(payload);
    try!(m.merge_from(&mut is));
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::ByteBuf;

    use super::*;
    use kvproto::eraftpb::{Message, MessageType};

    #[test]
    fn test_msg_codec() {
        let mut m1 = Message::new();
        m1.set_msg_type(MessageType::MsgBeat);

        let mut w = ByteBuf::mut_with_capacity(64);
        assert_eq!(encode_msg(&mut w, 1, &m1).is_ok(), true);

        let mut m2 = Message::new();
        assert_eq!(decode_msg(&mut w.flip(), &mut m2).unwrap(), 1);
        assert_eq!(m1, m2);
    }

    #[test]
    fn test_header_codec() {
        let m1 = encode_msg_header(1, 1);
        let mut m1_r = m1.as_slice();
        let (msg_id, payload_len) = decode_msg_header(&mut m1_r).unwrap();
        assert_eq!(msg_id, 1);
        assert_eq!(payload_len, 1);
    }
}

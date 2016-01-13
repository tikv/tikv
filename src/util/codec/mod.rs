use std::{result, io, fmt};
use std::error;
use std::convert;

use byteorder::{ByteOrder, BigEndian};
use protobuf;

pub const MSG_HEADER_LEN: usize = 16;
pub const MSG_MAGIC: u16 = 0xdaf4;
pub const MSG_VERSION_V1: u16 = 1;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Protobuf(protobuf::ProtobufError),
}


impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Error::Io(ref e) => fmt::Debug::fmt(e, f),
            &Error::Protobuf(ref e) => fmt::Debug::fmt(e, f),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            // not sure that cause should be included in message
            &Error::Io(ref e) => e.description(),
            &Error::Protobuf(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            &Error::Io(ref e) => Some(e),
            &Error::Protobuf(ref e) => Some(e),
        }
    }
}


impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<protobuf::ProtobufError> for Error {
    fn from(err: protobuf::ProtobufError) -> Error {
        Error::Protobuf(err)
    }
}

fn other_err(msg: String) -> Error {
    let err = io::Error::new(io::ErrorKind::Other, msg);
    convert::From::from(err)
}

pub type Result<T> = result::Result<T, Error>;


// Encodes message with message ID and protobuf body.
// Message contains header + payload.
// Header is 16 bytes, format:
//  | 0xdaf4 (2 bytes)| 0x01 (version 2 bytes)| msg_len (4 bytes) | msg_id (8 bytes)|, all use bigendian.
// Payload is a protobuf message.
pub fn encode_msg<T: io::Write, M: protobuf::Message>(w: &mut T,
                                                      msg_id: u64,
                                                      msg: &M)
                                                      -> Result<()> {
    // Create a 16 bytes header and write it.
    let mut buf = vec!(0; MSG_HEADER_LEN);
    BigEndian::write_u16(&mut buf[0..2], MSG_MAGIC);
    BigEndian::write_u16(&mut buf[2..4], MSG_VERSION_V1);
    BigEndian::write_u64(&mut buf[8..16], msg_id);

    try!(msg.write_to_vec(&mut buf));

    // Set payload length here
    let payload_len = (buf.len() - MSG_HEADER_LEN) as u32;
    BigEndian::write_u32(&mut buf[4..8], payload_len);

    try!(w.write(&buf));

    Ok(())
}

// Decode message header, return a tuple (message id, payload length);
pub fn decode_msg_header<T: io::Read>(r: &mut T) -> Result<(u64, usize)> {
    // First read 16 bytes header
    let mut header = [0; MSG_HEADER_LEN];
    // TODO: check exact read bytes number.
    try!(r.read(&mut header));

    let magic = BigEndian::read_u16(&header[0..2]);
    if MSG_MAGIC != magic {
        return Err(other_err(format!("invalid magic {}, not {}", magic, MSG_MAGIC)));
    }

    let version = BigEndian::read_u16(&header[2..4]);
    if MSG_VERSION_V1 != version {
        return Err(other_err(format!("unsupported version {}, we need {} now",
                                     version,
                                     MSG_VERSION_V1)));
    }

    let payload_len = BigEndian::read_u32(&header[4..8]) as usize;
    // TODO: check max payload

    let message_id = BigEndian::read_u64(&header[8..16]);

    Ok((message_id, payload_len))
}

// Decode message body with its payload length;
pub fn decode_msg_body<T: io::Read, M: protobuf::Message>(r: &mut T,
                                                          payload_len: usize,
                                                          m: &mut M)
                                                          -> Result<()> {
    let mut payload = vec![0;payload_len];

    try!(r.read(&mut payload));
    try!(m.merge_from_bytes(&payload));

    Ok(())
}

// Decodes message with above encoded format, returns message ID.
pub fn decode_msg<T: io::Read, M: protobuf::Message>(r: &mut T, m: &mut M) -> Result<u64> {
    let (message_id, payload_len) = try!(decode_msg_header(r));

    try!(decode_msg_body(r, payload_len, m));

    Ok(message_id)
}

#[cfg(test)]
mod tests {
    use bytes::ByteBuf;

    use super::*;
    use raft::raftpb::{Message, MessageType};

    #[test]
    fn test_codec() {
        let mut m1 = Message::new();
        m1.set_field_type(MessageType::MsgBeat);

        let mut w = ByteBuf::mut_with_capacity(64);
        assert_eq!(encode_msg(&mut w, 1, &m1).is_ok(), true);

        let mut m2 = Message::new();
        assert_eq!(decode_msg(&mut w.flip(), &mut m2).unwrap(), 1);
        assert_eq!(m1, m2);
    }
}

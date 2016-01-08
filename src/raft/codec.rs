use std::io;
use std::vec::Vec;

use byteorder::{ByteOrder, BigEndian};
use protobuf::{Message, MessageStatic};

use raft::errors::{Result, Error};

const MSG_HEADER_LEN: usize = 16;
const MSG_MAGIC: u16 = 0xdaf4;
const MSG_VERSION_V1: u16 = 1;

// Encodes message with message ID and protobuf body.
// Message contains header + payload.
// Header is 16 bytes, format:
//  | 0xdaf4 (2 bytes)| 0x01 (version 2 bytes)| msg_len (4 bytes| msg_id (8 bytes)|, all use bigendian.
// Payload is a protobuf message.
pub fn encode_message<T: io::Write, M: Message>(w: &mut T, msg_id: u64, msg: &M) -> Result<()> {
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

// Decodes message with above encoded format, returns message ID.
pub fn decode_message<T: io::Read, M: Message>(r: &mut T, m: &mut M) -> Result<u64> {
    // First read 16 bytes header
    let mut header = [0; MSG_HEADER_LEN];
    // TODO: check exact read bytes number.
    try!(r.read(&mut header));

    let magic = BigEndian::read_u16(&header[0..2]);
    if MSG_MAGIC != magic {
        // TODO: use our own error.
        return Err(Error::Other(format!("invalid magic {}, not {}", magic, MSG_MAGIC)));
    }

    let version = BigEndian::read_u16(&header[2..4]);
    if MSG_VERSION_V1 != version {
        // TODO: use our own error.
        return Err(Error::Other(format!("unsupported version {}, we need {} now",
                                        version,
                                        MSG_VERSION_V1)));
    }

    let payload_len = BigEndian::read_u32(&header[4..8]) as usize;
    // TODO: check max payload

    let msg_id: u64 = BigEndian::read_u64(&header[8..16]);
    let mut payload = vec![0;payload_len];

    let n = try!(r.read(&mut payload));
    try!(m.merge_from_bytes(&payload));

    Ok(msg_id)
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, ByteBuf, MutByteBuf};

    use raft::codec::{encode_message, decode_message};
    use raft::raftpb::{Message, MessageType};
    use raft::errors::{Result, Error};

    #[test]
    fn test_codec() {
        let mut m1 = Message::new();
        m1.set_field_type(MessageType::MsgBeat);

        let mut w = ByteBuf::mut_with_capacity(64);
        assert_eq!(encode_message(&mut w, 1, &m1).is_ok(), true);

        let mut m2 = Message::new();
        assert_eq!(decode_message(&mut w.flip(), &mut m2).unwrap(), 1);
        assert_eq!(m1, m2);
    }
}

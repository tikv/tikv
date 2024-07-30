// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use codec::prelude::*;
use ordered_float::OrderedFloat;

use crate::codec::Result;

// TODO: Implement generic version
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VectorFloat32 {
    pub value: Vec<OrderedFloat<f32>>,
}

impl VectorFloat32 {
    pub fn new(value: Vec<OrderedFloat<f32>>) -> Self {
        Self { value }
    }

    pub fn as_ref(&self) -> VectorFloat32Ref<'_> {
        VectorFloat32Ref {
            value: self.value.as_slice(),
        }
    }
}

impl std::fmt::Display for VectorFloat32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl std::fmt::Debug for VectorFloat32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

pub trait VectorFloat32DatumPayloadChunkEncoder: BufferWriter {
    fn write_vector_float32_to_chunk_by_datum_payload(&mut self, src_payload: &[u8]) -> Result<()> {
        // VectorFloat32's chunk format is same as binary format.
        self.write_bytes(src_payload)?;
        Ok(())
    }
}

impl<T: BufferWriter> VectorFloat32DatumPayloadChunkEncoder for T {}

impl<T: BufferWriter> VectorFloat32Encoder for T {}

pub trait VectorFloat32Decoder: NumberDecoder {
    // `read_vector_float32_ref` decodes value encoded by `write_vector_float32`
    // before.
    fn read_vector_float32_ref(&mut self) -> Result<VectorFloat32Ref<'_>> {
        if !cfg!(target_endian = "little") {
            return Err(box_err!("VectorFloat32 only support Little Endian"));
        }

        if self.bytes().is_empty() {
            return Ok(VectorFloat32Ref::new(&[]));
        }
        let n = self.read_u32_le()? as usize;
        let data_size = n * 4;
        let data = self.read_bytes(data_size)?;
        let data_in_f32 =
            unsafe { std::slice::from_raw_parts(data.as_ptr() as *const OrderedFloat<f32>, n) };
        Ok(VectorFloat32Ref::new(data_in_f32))
    }

    // `read_vector_float32` decodes value encoded by `write_vector_float32` before.
    fn read_vector_float32(&mut self) -> Result<VectorFloat32> {
        let r = self.read_vector_float32_ref()?;
        Ok(r.to_owned())
    }
}

impl<T: BufferReader> VectorFloat32Decoder for T {}

/// Represents a reference of VectorFloat32 value aiming to reduce memory copy.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VectorFloat32Ref<'a> {
    // Referred value
    value: &'a [OrderedFloat<f32>],
}

impl<'a> VectorFloat32Ref<'a> {
    pub fn new(value: &[OrderedFloat<f32>]) -> VectorFloat32Ref<'_> {
        VectorFloat32Ref { value }
    }

    pub fn encoded_len(&self) -> usize {
        self.value.len() * 4 + 4
    }

    pub fn to_owned(&self) -> VectorFloat32 {
        VectorFloat32 {
            value: self.value.to_owned(),
        }
    }

    pub fn len(&self) -> usize {
        self.value.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait VectorFloat32Encoder: NumberEncoder {
    fn write_vector_float32(&mut self, data: VectorFloat32Ref<'_>) -> Result<()> {
        if !cfg!(target_endian = "little") {
            return Err(box_err!("VectorFloat32 only support Little Endian"));
        }

        self.write_u32_le(data.value.len() as u32)?;
        unsafe {
            let data_in_bytes =
                std::slice::from_raw_parts(data.value.as_ptr() as *const u8, data.value.len() * 4);
            self.write_bytes(data_in_bytes)?;
        }
        Ok(())
    }
}

impl std::fmt::Display for VectorFloat32Ref<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, v) in self.value.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", v)?;
        }
        write!(f, "]")?;
        Ok(())
    }
}

impl std::fmt::Debug for VectorFloat32Ref<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl ToString for VectorFloat32Ref<'_> {
    fn to_string(&self) -> String {
        format!("{}", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_string() {
        let v = VectorFloat32::new(vec![OrderedFloat(1.0), OrderedFloat(2.0)]);
        assert_eq!("[1,2]", v.to_string());

        let v = VectorFloat32::new(vec![OrderedFloat(1.1), OrderedFloat(2.2)]);
        assert_eq!("[1.1,2.2]", v.to_string());

        let v = VectorFloat32::new(vec![]);
        assert_eq!("[]", v.to_string());
    }

    #[test]
    fn test_encode() {
        let v = VectorFloat32::new(vec![OrderedFloat(1.1), OrderedFloat(2.2)]);
        let mut encoded = Vec::new();
        encoded.write_vector_float32(v.as_ref()).unwrap();
        assert_eq!(
            encoded,
            vec![
                0x02, 0x00, 0x00, 0x00, // Length = 0x02
                0xcd, 0xcc, 0x8c, 0x3f, // Element 1 = 0x3f8ccccd
                0xcd, 0xcc, 0x0c, 0x40, // Element 2 = 0x400ccccd
            ]
        );
        assert_eq!(v.as_ref().encoded_len(), 12);
    }

    #[test]
    fn test_decode() {
        let buf: Vec<u8> = vec![
            0x02, 0x00, 0x00, 0x00, // Length = 0x02
            0xcd, 0xcc, 0x8c, 0x3f, // Element 1 = 0x3f8ccccd
            0xcd, 0xcc, 0x0c, 0x40, // Element 2 = 0x400ccccd
            0xff, // Remaining dummy data
        ];

        let mut buf_slice = &buf[..];
        let v = buf_slice.read_vector_float32_ref().unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v.to_string(), "[1.1,2.2]");
        assert_eq!(buf_slice.len(), 1);
        assert_eq!(buf_slice, &[0xff]);

        buf_slice.read_vector_float32_ref().unwrap_err();
        assert_eq!(buf_slice.len(), 1);
        assert_eq!(buf_slice, &[0xff]);

        buf_slice = &[];
        let v = buf_slice.read_vector_float32_ref().unwrap();
        assert_eq!(v.len(), 0);
        assert_eq!(v.to_string(), "[]");
        let mut encode_buf = Vec::new();
        encode_buf.write_vector_float32(v).unwrap();
        assert_eq!(encode_buf, vec![0x00, 0x00, 0x00, 0x00]);
    }
}

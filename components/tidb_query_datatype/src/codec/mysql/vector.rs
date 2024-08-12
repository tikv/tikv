// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use codec::prelude::*;

use crate::codec::Result;

const F32_SIZE: usize = std::mem::size_of::<f32>();

// TODO: Implement generic version
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct VectorFloat32 {
    // Use Vec<u8> instead of Vec<f32> to avoid reading from unaligned bytes. For example,
    // bytes read from protobuf is usually not aligned by f32.
    pub value: Vec<u8>,
}

impl Ord for VectorFloat32 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().cmp(&other.as_ref())
    }
}

impl PartialOrd for VectorFloat32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl VectorFloat32 {
    pub fn new(value: Vec<u8>) -> Result<Self> {
        _ = VectorFloat32Ref::new(&value[..])?;
        Ok(VectorFloat32 { value })
    }

    pub fn from_f32(value: Vec<f32>) -> Result<Self> {
        let value_u8: &[u8] = bytemuck::cast_slice(&value);
        VectorFloat32::new(value_u8.to_owned())
    }

    pub fn as_ref(&self) -> VectorFloat32Ref<'_> {
        VectorFloat32Ref { value: &self.value }
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
            return VectorFloat32Ref::new(&[]);
        }
        let n = self.read_u32_le()? as usize;
        let data_size = n * F32_SIZE;
        let data = self.read_bytes(data_size)?;
        VectorFloat32Ref::new(data)
    }

    // `read_vector_float32` decodes value encoded by `write_vector_float32` before.
    fn read_vector_float32(&mut self) -> Result<VectorFloat32> {
        let r = self.read_vector_float32_ref()?;
        Ok(r.to_owned())
    }
}

impl<T: BufferReader> VectorFloat32Decoder for T {}

/// Represents a reference of VectorFloat32 value aiming to reduce memory copy.
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct VectorFloat32Ref<'a> {
    // Use &[u8] instead of &[u32] to allow reading from unaligned bytes. For example,
    // bytes read from protobuf is usually not aligned by f32.
    value: &'a [u8],
}

impl<'a> Ord for VectorFloat32Ref<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        let la = self.len();
        let lb = other.len();
        let common_len = std::cmp::min(la, lb);
        for i in 0..common_len {
            if self.index(i) > other.index(i) {
                return Ordering::Greater;
            } else if self.index(i) < other.index(i) {
                return Ordering::Less;
            }
        }
        la.cmp(&lb)
    }
}

impl<'a> PartialOrd for VectorFloat32Ref<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> VectorFloat32Ref<'a> {
    pub fn new(value: &[u8]) -> Result<VectorFloat32Ref<'_>> {
        if value.len() % F32_SIZE != 0 {
            return Err(box_err!("Vector length error. Please check the input."));
        }
        let check_vec = VectorFloat32Ref { value };
        for i in 0..check_vec.len() {
            if check_vec.index(i).is_nan() {
                return Err(box_err!("NaN not allowed in vector"));
            }
            if check_vec.index(i).is_infinite() {
                return Err(box_err!("infinite value not allowed in vector"));
            }
        }
        Ok(check_vec)
    }

    pub fn from_f32(value: &[f32]) -> Result<VectorFloat32Ref<'_>> {
        let vec_u8: &[u8] = bytemuck::cast_slice(value);
        VectorFloat32Ref::new(vec_u8)
    }

    pub fn encoded_len(&self) -> usize {
        self.value.len() + std::mem::size_of::<u32>()
    }

    pub fn to_owned(&self) -> VectorFloat32 {
        VectorFloat32 {
            value: self.value.to_owned(),
        }
    }

    pub fn len(&self) -> usize {
        self.value.len() / F32_SIZE
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn check_dims(&self, b: VectorFloat32Ref<'a>) -> Result<()> {
        if self.len() != b.len() {
            return Err(box_err!(
                "vectors have different dimensions: {} and {}",
                self.len(),
                b.len()
            ));
        }
        Ok(())
    }

    fn index(&self, idx: usize) -> f32 {
        let byte_index: usize = idx * F32_SIZE;
        if byte_index + F32_SIZE > self.value.len() {
            panic!(
                "Index out of bounds: index = {}, length = {}",
                idx,
                self.len()
            );
        }
        let float_ptr = unsafe { self.value.as_ptr().add(byte_index) as *const f32 };
        unsafe { float_ptr.read_unaligned() }
    }

    pub fn l2_squared_distance(&self, b: VectorFloat32Ref<'a>) -> Result<f64> {
        self.check_dims(b)?;
        let mut distance: f32 = 0.0;

        for i in 0..self.len() {
            let diff = self.index(i) - b.index(i);
            distance += diff * diff;
        }

        Ok(distance as f64)
    }

    pub fn l2_distance(&self, b: VectorFloat32Ref<'a>) -> Result<f64> {
        Ok(self.l2_squared_distance(b)?.sqrt())
    }

    pub fn inner_product(&self, b: VectorFloat32Ref<'a>) -> Result<f64> {
        self.check_dims(b)?;
        let mut distance: f32 = 0.0;
        for i in 0..self.len() {
            distance += self.index(i) * b.index(i);
        }

        Ok(distance as f64)
    }

    pub fn cosine_distance(&self, b: VectorFloat32Ref<'a>) -> Result<f64> {
        self.check_dims(b)?;
        let mut distance: f32 = 0.0;
        let mut norma: f32 = 0.0;
        let mut normb: f32 = 0.0;
        for i in 0..self.len() {
            distance += self.index(i) * b.index(i);
            norma += self.index(i) * self.index(i);
            normb += b.index(i) * b.index(i);
        }

        let similarity = (distance as f64) / ((norma as f64) * (normb as f64)).sqrt();
        if similarity.is_nan() {
            // Divide by zero
            return Ok(std::f64::NAN);
        }
        let similarity = similarity.clamp(-1.0, 1.0);
        Ok(1.0 - similarity)
    }

    pub fn l1_distance(&self, b: VectorFloat32Ref<'a>) -> Result<f64> {
        self.check_dims(b)?;
        let mut distance: f32 = 0.0;
        for i in 0..self.len() {
            let diff = self.index(i) - b.index(i);
            distance += diff.abs();
        }

        Ok(distance as f64)
    }

    pub fn l2_norm(&self) -> f64 {
        // Note: We align the impl with pgvector: Only l2_norm use double
        // precision during calculation.
        let mut norm: f64 = 0.0;
        for i in 0..self.len() {
            let v = self.index(i) as f64;
            norm += v * v;
        }

        norm.sqrt()
    }
}

pub trait VectorFloat32Encoder: NumberEncoder {
    fn write_vector_float32(&mut self, data: VectorFloat32Ref<'_>) -> Result<()> {
        if !cfg!(target_endian = "little") {
            return Err(box_err!("VectorFloat32 only support Little Endian"));
        }

        self.write_u32_le(data.len() as u32)?;
        self.write_bytes(data.value)?;

        Ok(())
    }
}

impl std::fmt::Display for VectorFloat32Ref<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        let mut is_first = true;
        for i in 0..self.len() {
            if is_first {
                write!(f, "{}", self.index(i))?;
                is_first = false;
            } else {
                write!(f, ",{}", self.index(i))?;
            }
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
    fn test_nan_inf() {
        let v = VectorFloat32::from_f32(vec![1.0, std::f32::NAN]);
        v.unwrap_err();

        let v = VectorFloat32::from_f32(vec![1.0, std::f32::INFINITY]);
        v.unwrap_err();

        let v = VectorFloat32::from_f32(vec![1.0, std::f32::NEG_INFINITY]);
        v.unwrap_err();

        let v = VectorFloat32Ref::from_f32(&[1.0, std::f32::NAN]);
        v.unwrap_err();

        let v = VectorFloat32Ref::from_f32(&[1.0, std::f32::INFINITY]);
        v.unwrap_err();

        let v = VectorFloat32Ref::from_f32(&[1.0, std::f32::NEG_INFINITY]);
        v.unwrap_err();
    }

    #[test]
    fn test_to_string() {
        let v = VectorFloat32::from_f32(vec![1.0, 2.0]).unwrap();
        assert_eq!("[1,2]", v.to_string());

        let v = VectorFloat32::from_f32(vec![1.1, 2.2]).unwrap();
        assert_eq!("[1.1,2.2]", v.to_string());

        let v = VectorFloat32::from_f32(vec![]).unwrap();
        assert_eq!("[]", v.to_string());
    }

    #[test]
    fn test_input_length() {
        let buf: Vec<u8> = vec![
            0xcd, 0xcc, 0x8c, 0x3f, // Element 1 = 0x3f8ccccd
            0xcd, 0xcc, 0x0c, 0x40, // Element 2 = 0x400ccccd
            0xcd, 0xcc, 0x0c,
        ];
        let v = VectorFloat32Ref::new(&buf[..]);
        v.unwrap_err();

        let buf: Vec<u8> = vec![
            0xcd, 0xcc, 0x8c, 0x3f, // Element 1 = 0x3f8ccccd
            0xcd, 0xcc, 0x0c, 0x40, // Element 2 = 0x400ccccd
            0xcd, 0xcc, 0x0c, 0x40,
        ];
        let v = VectorFloat32Ref::new(&buf[..]);
        v.unwrap();
    }

    #[test]
    fn test_compare() {
        let v1 = VectorFloat32::from_f32(vec![1.0, 2.0]).unwrap();
        let v2 = VectorFloat32::from_f32(vec![1.1, 2.2]).unwrap();
        assert!(v1 < v2);

        let v3 = VectorFloat32::from_f32(vec![1.0, 2.0]).unwrap();
        assert!(v1 == v3);

        let v4 = VectorFloat32::from_f32(vec![0.3, 0.4]).unwrap();
        assert!(v1 > v4);

        let v4 = VectorFloat32::from_f32(vec![1.0]).unwrap();
        assert!(v1 > v4);

        let v5 = VectorFloat32::from_f32(vec![1.0, 2.0, 0.5]).unwrap();
        assert!(v1 < v5);
    }

    #[test]
    fn test_encode() {
        let v = VectorFloat32::from_f32(vec![1.1, 2.2]).unwrap();
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

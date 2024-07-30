// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use codec::prelude::*;
use ordered_float::NotNan;

use crate::codec::Result;

// TODO: Implement generic version
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VectorFloat32 {
    pub value: Vec<NotNan<f32>>,
}

impl VectorFloat32 {
    pub fn new(value: Vec<f32>) -> Result<Self> {
        // Check again that NaN and ±Inf is not contained
        for v in &value {
            if v.is_nan() {
                return Err(box_err!("NaN not allowed in vector"));
            }
            if v.is_infinite() {
                return Err(box_err!("infinite value not allowed in vector"));
            }
        }
        Ok(Self {
            value: value
                .into_iter()
                .map(|v| NotNan::<f32>::new(v).unwrap())
                .collect(),
        })
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
            return VectorFloat32Ref::new(&[]);
        }
        let n = self.read_u32_le()? as usize;
        let data_size = n * 4;
        let data = self.read_bytes(data_size)?;
        let data_in_f32 = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const f32, n) };
        VectorFloat32Ref::new(data_in_f32)
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
    value: &'a [NotNan<f32>],
}

impl<'a> VectorFloat32Ref<'a> {
    pub fn new(value: &[f32]) -> Result<VectorFloat32Ref<'_>> {
        // Check again that NaN and ±Inf is not contained
        for v in value {
            if v.is_nan() {
                return Err(box_err!("NaN not allowed in vector"));
            }
            if v.is_infinite() {
                return Err(box_err!("infinite value not allowed in vector"));
            }
        }
        let value_in_notnan: &[NotNan<f32>] = unsafe {
            std::slice::from_raw_parts(value.as_ptr() as *const NotNan<f32>, value.len())
        };
        Ok(VectorFloat32Ref {
            value: value_in_notnan,
        })
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

    fn primitive_value(&self) -> &[f32] {
        unsafe { std::slice::from_raw_parts(self.value.as_ptr() as *const f32, self.value.len()) }
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

    pub fn l2_squared_distance(&self, b: VectorFloat32Ref<'a>) -> Result<f64> {
        self.check_dims(b)?;
        let mut distance: f32 = 0.0;

        // Note: Must NOT calculate over va[i].value and vb[i].value directly (which is
        // a &[NotNan]), because it will panic if the calculated result is NaN.
        // See https://docs.rs/ordered-float/latest/ordered_float/struct.NotNan.html
        let va = self.primitive_value();
        let vb = b.primitive_value();
        for i in 0..self.len() {
            // Hope this can be vectorized.
            let diff = va[i] - vb[i];
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
        let va = self.primitive_value();
        let vb = b.primitive_value();
        for i in 0..self.len() {
            // Hope this can be vectorized.
            distance += va[i] * vb[i];
        }
        Ok(distance as f64)
    }

    pub fn cosine_distance(&self, b: VectorFloat32Ref<'a>) -> Result<f64> {
        self.check_dims(b)?;
        let mut distance: f32 = 0.0;
        let mut norma: f32 = 0.0;
        let mut normb: f32 = 0.0;
        let va = self.primitive_value();
        let vb = b.primitive_value();
        for i in 0..self.len() {
            // Hope this can be vectorized.
            distance += va[i] * vb[i];
            norma += va[i] * va[i];
            normb += vb[i] * vb[i];
        }
        let similarity = (distance as f64) / ((norma as f64).sqrt() * (normb as f64).sqrt());
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
        let va = self.primitive_value();
        let vb = b.primitive_value();
        for i in 0..self.len() {
            // Hope this can be vectorized.
            let diff = va[i] - vb[i];
            distance += diff.abs();
        }
        Ok(distance as f64)
    }

    pub fn l2_norm(&self) -> f64 {
        // Note: We align the impl with pgvector: Only l2_norm use double
        // precision during calculation.
        let mut norm: f64 = 0.0;
        let va = self.primitive_value();
        for i in 0..self.len() {
            // Hope this can be vectorized.
            norm += va[i] as f64 * va[i] as f64;
        }
        norm.sqrt()
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
    fn test_nan_inf() {
        let v = VectorFloat32::new(vec![1.0, std::f32::NAN]);
        v.unwrap_err();

        let v = VectorFloat32::new(vec![1.0, std::f32::INFINITY]);
        v.unwrap_err();

        let v = VectorFloat32::new(vec![1.0, std::f32::NEG_INFINITY]);
        v.unwrap_err();

        let v = VectorFloat32Ref::new(&[1.0, std::f32::NAN]);
        v.unwrap_err();

        let v = VectorFloat32Ref::new(&[1.0, std::f32::INFINITY]);
        v.unwrap_err();

        let v = VectorFloat32Ref::new(&[1.0, std::f32::NEG_INFINITY]);
        v.unwrap_err();
    }

    #[test]
    fn test_to_string() {
        let v = VectorFloat32::new(vec![1.0, 2.0]).unwrap();
        assert_eq!("[1,2]", v.to_string());

        let v = VectorFloat32::new(vec![1.1, 2.2]).unwrap();
        assert_eq!("[1.1,2.2]", v.to_string());

        let v = VectorFloat32::new(vec![]).unwrap();
        assert_eq!("[]", v.to_string());
    }

    #[test]
    fn test_encode() {
        let v = VectorFloat32::new(vec![1.1, 2.2]).unwrap();
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
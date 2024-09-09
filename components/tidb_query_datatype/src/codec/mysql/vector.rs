// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(target_arch = "aarch64")]
use core::arch::aarch64::*;
#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::*;

use codec::prelude::*;
use ordered_float::OrderedFloat;
use simsimd::SpatialSimilarity;
use static_assertions::assert_eq_size;

use crate::codec::Result;

const LEN_PREFIX_SIZE: usize = std::mem::size_of::<u32>();
const ELEMENT_SIZE: usize = std::mem::size_of::<f32>();

/// Aligned, Owned.
#[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct VectorFloat32 {
    pub value: Vec<OrderedFloat<f32>>, // Data must be aligned
}

impl VectorFloat32 {
    pub fn copy_from_f32(value: &[f32]) -> Self {
        let ordered_value: Vec<OrderedFloat<f32>> =
            value.iter().map(|v| OrderedFloat(*v)).collect();
        VectorFloat32 {
            value: ordered_value,
        }
    }

    #[inline]
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

/// Aligned, Ref.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VectorFloat32Ref<'a> {
    value: &'a [OrderedFloat<f32>], // Data must be aligned
}

impl<'a> std::ops::Index<usize> for VectorFloat32Ref<'a> {
    type Output = f32;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.value[index].0
    }
}

assert_eq_size!(OrderedFloat<f32>, f32);

impl<'a> VectorFloat32Ref<'a> {
    pub fn from_f32(value: &[f32]) -> VectorFloat32Ref<'_> {
        // OrderedFloat is POD, so it is safe.
        let ordered_value = unsafe {
            std::slice::from_raw_parts(value.as_ptr() as *const OrderedFloat<f32>, value.len())
        };
        VectorFloat32Ref {
            value: ordered_value,
        }
    }

    #[inline]
    pub fn data(&self) -> &[f32] {
        // OrderedFloat is POD, so it is safe.
        unsafe { std::slice::from_raw_parts(self.value.as_ptr() as *const f32, self.value.len()) }
    }

    #[inline]
    pub fn encoded_len(&self) -> usize {
        self.value.len() * ELEMENT_SIZE + LEN_PREFIX_SIZE
    }

    #[inline]
    pub fn to_owned(&self) -> VectorFloat32 {
        VectorFloat32 {
            value: self.value.to_owned(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.value.len()
    }

    #[inline]
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
}

impl std::fmt::Display for VectorFloat32Ref<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for i in 0..self.len() {
            if i == 0 {
                write!(f, "{}", self[i])?;
            } else {
                write!(f, ",{}", self[i])?;
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

// Vector distance and functions
impl<'a> VectorFloat32Ref<'a> {
    pub fn l2_squared_distance(&self, b: VectorFloat32Ref<'a>) -> Result<f64> {
        self.check_dims(b)?;
        let l2_distance =
            f32::sqeuclidean(self.data(), b.data()).expect("Vectors must be of the same length");
        Ok(l2_distance)
    }

    pub fn l2_distance(&self, b: VectorFloat32Ref<'a>) -> Result<f64> {
        Ok(self.l2_squared_distance(b)?.sqrt())
    }

    pub fn inner_product(&self, b: VectorFloat32Ref<'a>) -> Result<f64> {
        self.check_dims(b)?;
        let inner_product = SpatialSimilarity::dot(self.data(), b.data())
            .expect("Vectors must be of the same length");
        Ok(inner_product)
    }

    pub fn cosine_distance(&self, b: VectorFloat32Ref<'a>) -> Result<f64> {
        self.check_dims(b)?;
        #[cfg(target_arch = "x86_64")]
        {
            let mut distance: f32 = 0.0;
            let mut norma: f32 = 0.0;
            let mut normb: f32 = 0.0;

            unsafe {
                let mut i = 0;
                while i + 4 <= self.len() {
                    // load 4 float-elements to simd register.
                    let a_vec = _mm512_loadu_ps(self.data().as_ptr().add(i));
                    let b_vec = _mm_loadu_ps(b.data().as_ptr().add(i));

                    let ab_vec = _mm_mul_ps(a_vec, b_vec);
                    let a_sq_vec = _mm_mul_ps(a_vec, a_vec);
                    let b_sq_vec = _mm_mul_ps(b_vec, b_vec);

                    distance += _mm_cvtss_f32(_mm_hadd_ps(ab_vec, _mm_setzero_ps()))
                        + _mm_cvtss_f32(_mm_hadd_ps(
                            _mm_movehl_ps(ab_vec, ab_vec),
                            _mm_setzero_ps(),
                        ));

                    norma += _mm_cvtss_f32(_mm_hadd_ps(a_sq_vec, _mm_setzero_ps()))
                        + _mm_cvtss_f32(_mm_hadd_ps(
                            _mm_movehl_ps(a_sq_vec, a_sq_vec),
                            _mm_setzero_ps(),
                        ));

                    normb += _mm_cvtss_f32(_mm_hadd_ps(b_sq_vec, _mm_setzero_ps()))
                        + _mm_cvtss_f32(_mm_hadd_ps(
                            _mm_movehl_ps(b_sq_vec, b_sq_vec),
                            _mm_setzero_ps(),
                        ));

                    i += 4;
                }

                // process remaining element
                for j in i..self.len() {
                    distance += self[j] * b[j];
                    norma += self[j] * self[j];
                    normb += b[j] * b[j];
                }
            }

            let similarity = (distance as f64) / ((norma as f64) * (normb as f64)).sqrt();
            if similarity.is_nan() {
                return Ok(std::f64::NAN);
            }
            let similarity = similarity.clamp(-1.0, 1.0);
            Ok(1.0 - similarity)
        }

        #[cfg(target_arch = "aarch64")]
        {
            let mut distance: f32 = 0.0;
            let mut norma: f32 = 0.0;
            let mut normb: f32 = 0.0;

            unsafe {
                let mut i = 0;
                while i + 4 <= self.len() {
                    // load 4 float-elements to simd register.
                    let a_vec = vld1q_f32(self.data().as_ptr().add(i));
                    let b_vec = vld1q_f32(b.data().as_ptr().add(i));

                    let ab_vec = vmulq_f32(a_vec, b_vec);
                    let a_sq_vec = vmulq_f32(a_vec, a_vec);
                    let b_sq_vec = vmulq_f32(b_vec, b_vec);

                    distance += vaddvq_f32(ab_vec);
                    norma += vaddvq_f32(a_sq_vec);
                    normb += vaddvq_f32(b_sq_vec);

                    i += 4;
                }

                // process remaining element
                for j in i..self.len() {
                    distance += self[j] * b[j];
                    norma += self[j] * self[j];
                    normb += b[j] * b[j];
                }
            }

            let similarity = (distance as f64) / ((norma as f64) * (normb as f64)).sqrt();
            if similarity.is_nan() {
                return Ok(std::f64::NAN);
            }
            let similarity = similarity.clamp(-1.0, 1.0);
            Ok(1.0 - similarity)
        }
    }

    pub fn l1_distance(&self, b: VectorFloat32Ref<'a>) -> Result<f64> {
        self.check_dims(b)?;
        #[cfg(target_arch = "x86_64")]
        {
            let mut distance: f32 = 0.0;
            unsafe {
                let mut i = 0;
                while i + 4 <= self.len() {
                    let a_vec = _mm_loadu_ps(self.data().as_ptr().add(i));
                    let b_vec = _mm_loadu_ps(b.data().as_ptr().add(i));

                    let diff = _mm_sub_ps(a_vec, b_vec);

                    let abs_diff = _mm_andnot_ps(_mm_set1_ps(-0.0), diff);

                    let sum1 = _mm_hadd_ps(abs_diff, abs_diff);
                    let sum2 = _mm_hadd_ps(sum1, sum1);

                    distance += _mm_cvtss_f32(sum2);

                    i += 4;
                }
                // process remaining element
                for j in i..self.len() {
                    let diff = self[j] - b[j];
                    distance += diff.abs();
                }
            }
            Ok(distance as f64)
        }

        #[cfg(target_arch = "aarch64")]
        {
            let mut distance: f32 = 0.0;
            unsafe {
                let mut i = 0;
                while i + 4 <= self.len() {
                    // load 4 float-elements to simd register.
                    let a_vec = vld1q_f32(self.data().as_ptr().add(i));
                    let b_vec = vld1q_f32(b.data().as_ptr().add(i));

                    let diff = vsubq_f32(a_vec, b_vec);

                    let abs_diff = vabsq_f32(diff);

                    distance += vaddvq_f32(abs_diff);

                    i += 4;
                }
                // process remaining element
                for j in i..self.len() {
                    let diff = self[j] - b[j];
                    distance += diff.abs();
                }
            }
            Ok(distance as f64)
        }
    }

    pub fn l2_norm(&self) -> f64 {
        // Note: We align the impl with pgvector: Only l2_norm use double
        // precision during calculation.
        let mut norm: f64 = 0.0;
        for i in 0..self.len() {
            let v = self[i] as f64;
            norm += v * v;
        }

        norm.sqrt()
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

/// Misaligned, Ref.
#[derive(Clone, Copy)]
pub struct VectorFloat32RefMisaligned<'a> {
    value: &'a [u8], // Data could be notaligned. Does not contain length prefix.
}

impl<'a> VectorFloat32RefMisaligned<'a> {
    #[inline]
    pub fn len(&self) -> usize {
        self.value.len() / ELEMENT_SIZE
    }

    /// It's safe to convert a misaligned ref to an aligned owned value. We will
    /// copy data with proper alignment.
    pub fn to_owned(&self) -> VectorFloat32 {
        let mut aligned_data: Vec<OrderedFloat<f32>> = Vec::new();
        aligned_data.resize(self.len(), OrderedFloat(0.0f32));
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.value.as_ptr(),              // Aligned by u8
                aligned_data.as_mut_ptr().cast(), // Aligned by f32 → u8
                self.value.len(),
            )
        }
        VectorFloat32 {
            value: aligned_data,
        }
    }
}

pub trait VectorFloat32Decoder: NumberDecoder {
    // `read_vector_float32_ref` decodes value encoded by `write_vector_float32`
    // before.
    fn read_vector_float32_ref(&mut self) -> Result<VectorFloat32RefMisaligned<'_>> {
        if !cfg!(target_endian = "little") {
            return Err(box_err!("VectorFloat32 only support Little Endian"));
        }

        if self.bytes().is_empty() {
            return Ok(VectorFloat32RefMisaligned { value: &[] });
        }
        let n = self.read_u32_le()? as usize;
        let data_size = n * ELEMENT_SIZE;
        let data = self.read_bytes(data_size)?;
        Ok(VectorFloat32RefMisaligned { value: data })
    }

    // `read_vector_float32` decodes value encoded by `write_vector_float32` before.
    fn read_vector_float32(&mut self) -> Result<VectorFloat32> {
        let r = self.read_vector_float32_ref()?;
        Ok(r.to_owned())
    }
}

impl<T: BufferReader> VectorFloat32Decoder for T {}

pub trait VectorFloat32Encoder: NumberEncoder {
    fn write_vector_float32(&mut self, data: VectorFloat32Ref<'_>) -> Result<()> {
        if !cfg!(target_endian = "little") {
            return Err(box_err!("VectorFloat32 only support Little Endian"));
        }
        self.write_u32_le(data.len() as u32)?;
        self.write_bytes(bytemuck::cast_slice(data.data()))?;
        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_to_string() {
//         let v = VectorFloat32::from_f32(&[1.0, 2.0]);
//         assert_eq!("[1,2]", v.to_string());

//         let v = VectorFloat32::from_f32(&[1.1, 2.2]);
//         assert_eq!("[1.1,2.2]", v.to_string());

//         let v = VectorFloat32::from_f32(&[]);
//         assert_eq!("[]", v.to_string());
//     }

//     #[test]
//     fn test_input_length() {
//         let buf: Vec<u8> = vec![
//             0xcd, 0xcc, 0x8c, 0x3f, // Element 1 = 0x3f8ccccd
//             0xcd, 0xcc, 0x0c, 0x40, // Element 2 = 0x400ccccd
//             0xcd, 0xcc, 0x0c,
//         ];
//         let v = VectorFloat32Ref::new(&buf[..]);
//         v.unwrap_err();

//         let buf: Vec<u8> = vec![
//             0xcd, 0xcc, 0x8c, 0x3f, // Element 1 = 0x3f8ccccd
//             0xcd, 0xcc, 0x0c, 0x40, // Element 2 = 0x400ccccd
//             0xcd, 0xcc, 0x0c, 0x40,
//         ];
//         let v = VectorFloat32Ref::new(&buf[..]);
//         v.unwrap();
//     }

//     #[test]
//     fn test_compare() {
//         let v1 = VectorFloat32::from_f32(&[1.0, 2.0]);
//         let v2 = VectorFloat32::from_f32(&[1.1, 2.2]);
//         assert!(v1 < v2);

//         let v3 = VectorFloat32::from_f32(&[1.0, 2.0]);
//         assert!(v1 == v3);

//         let v4 = VectorFloat32::from_f32(&[0.3, 0.4]);
//         assert!(v1 > v4);

//         let v4 = VectorFloat32::from_f32(&[1.0]);
//         assert!(v1 > v4);

//         let v5 = VectorFloat32::from_f32(&[1.0, 2.0, 0.5]);
//         assert!(v1 < v5);
//     }

//     #[test]
//     fn test_encode() {
//         let v = VectorFloat32::from_f32(&[1.1, 2.2]);
//         let mut encoded = Vec::new();

//         encoded.write_vector_float32(v.as_ref()).unwrap();
//         assert_eq!(
//             encoded,
//             vec![
//                 0x02, 0x00, 0x00, 0x00, // Length = 0x02
//                 0xcd, 0xcc, 0x8c, 0x3f, // Element 1 = 0x3f8ccccd
//                 0xcd, 0xcc, 0x0c, 0x40, // Element 2 = 0x400ccccd
//             ]
//         );
//         assert_eq!(v.as_ref().encoded_len(), 12);
//     }

//     #[test]
//     fn test_decode() {
//         let buf: Vec<u8> = vec![
//             0x02, 0x00, 0x00, 0x00, // Length = 0x02
//             0xcd, 0xcc, 0x8c, 0x3f, // Element 1 = 0x3f8ccccd
//             0xcd, 0xcc, 0x0c, 0x40, // Element 2 = 0x400ccccd
//             0xff, // Remaining dummy data
//         ];

//         let mut buf_slice = &buf[..];

//         let v = buf_slice.read_vector_float32_ref().unwrap();

//         assert_eq!(v.len(), 2);
//         assert_eq!(v.to_string(), "[1.1,2.2]");

//         assert_eq!(buf_slice.len(), 1);
//         assert_eq!(buf_slice, &[0xff]);

//         buf_slice.read_vector_float32_ref().unwrap_err();
//         assert_eq!(buf_slice.len(), 1);
//         assert_eq!(buf_slice, &[0xff]);

//         buf_slice = &[];
//         let v = buf_slice.read_vector_float32_ref().unwrap();
//         assert_eq!(v.len(), 0);
//         assert_eq!(v.to_string(), "[]");
//         let mut encode_buf = Vec::new();
//         encode_buf.write_vector_float32(v).unwrap();
//         assert_eq!(encode_buf, vec![0x00, 0x00, 0x00, 0x00]);
//     }
// }

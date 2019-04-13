// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Index;

use super::*;

/// A vector-like value reference container, for all concrete eval types.
///
/// Vector-like: This type contains either a vector reference or a scalar reference. When inner
/// reference is a scalar value, it acts like a vector that containing arbitrary amount of same
/// elements. This capability is provided via specialized version.
///
/// This type is similar to a trait object but is faster due to function inlining.
// TODO: Switch to use trait object when trait object is faster.
#[derive(Copy, Clone)]
pub enum VectorLikeValueRef<'a> {
    Vector(&'a VectorValue),
    Scalar(&'a ScalarValue),
}

macro_rules! impl_specialize {
    ($ty:tt, $name:ident) => {
        impl<'a> VectorLikeValueRef<'a> {
            /// Converts this reference container to a concrete type specialized reference
            /// container.
            #[inline]
            pub fn $name(self) -> VectorLikeValueRefSpecialized<'a, $ty> {
                match self {
                    VectorLikeValueRef::Vector(v) => {
                        VectorLikeValueRefSpecialized::Vector(v.as_ref())
                    }
                    VectorLikeValueRef::Scalar(s) => {
                        VectorLikeValueRefSpecialized::Scalar(s.as_ref())
                    }
                }
            }
        }

        impl<'a> From<VectorLikeValueRef<'a>> for VectorLikeValueRefSpecialized<'a, $ty> {
            #[inline]
            fn from(v: VectorLikeValueRef<'a>) -> Self {
                v.$name()
            }
        }
    };
}

impl_specialize! { Int, specialize_as_int }
impl_specialize! { Real, specialize_as_real }
impl_specialize! { Decimal, specialize_as_decimal }
impl_specialize! { Bytes, specialize_as_bytes }
impl_specialize! { DateTime, specialize_as_date_time }
impl_specialize! { Duration, specialize_as_duration }
impl_specialize! { Json, specialize_as_json }

/// A concrete eval type specialized vector-like value reference container.
///
/// Vector-like: This type contains either a concrete vector reference or a concrete scalar
/// reference. When inner reference is a concrete scalar value, it acts like a concrete vector that
/// containing arbitrary amount of same elements.
///
/// When the concrete type of `VectorLikeValueRef` is known, it can be converted (specialized) into
/// this type so that repeated access over this type later won't pay for type checks.
#[derive(Copy, Clone)]
pub enum VectorLikeValueRefSpecialized<'a, T> {
    Vector(&'a [Option<T>]),
    Scalar(&'a Option<T>),
}

impl<'a, T> Index<usize> for VectorLikeValueRefSpecialized<'a, T> {
    type Output = Option<T>;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        match self {
            VectorLikeValueRefSpecialized::Vector(ref v) => &v[index],
            VectorLikeValueRefSpecialized::Scalar(ref v) => &v,
        }
    }
}

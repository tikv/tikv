// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Index;

use crate::coprocessor::codec::data_type::*;

/// A view to a logical vector, based on a physical vector value or a scalar value.
///
/// When view is built over a scalar value, it acts like a vector that containing arbitrary
/// amount of same scalar value elements.
#[derive(Copy, Clone)]
pub enum LogicalVectorView<'a> {
    Vector {
        physical_value: &'a VectorValue,
        logical_rows: &'a [usize],
    },
    Scalar(&'a ScalarValue),
}

impl<'a> LogicalVectorView<'a> {
    /// Builds the view from a physical vector value.
    pub fn from_physical_vector(
        physical_value: &'a VectorValue,
        logical_rows: &'a [usize],
    ) -> Self {
        LogicalVectorView::Vector {
            physical_value,
            logical_rows,
        }
    }

    /// Builds the view from a scalar value.
    pub fn from_scalar(scalar_value: &'a ScalarValue) -> Self {
        LogicalVectorView::Scalar(scalar_value)
    }
}

macro_rules! impl_into_view {
    ($ty:tt, $name:ident) => {
        impl<'a> LogicalVectorView<'a> {
            /// Converts this reference container to a concrete type specialized reference
            /// container.
            #[inline]
            pub fn $name(self) -> ConcreteLogicalVectorView<'a, $ty> {
                match self {
                    LogicalVectorView::Vector {
                        physical_value,
                        logical_rows,
                    } => ConcreteLogicalVectorView::Vector {
                        physical_value: physical_value.as_ref(),
                        logical_rows,
                    },
                    LogicalVectorView::Scalar(s) => ConcreteLogicalVectorView::Scalar(s.as_ref()),
                }
            }
        }

        impl<'a> From<LogicalVectorView<'a>> for ConcreteLogicalVectorView<'a, $ty> {
            #[inline]
            fn from(v: LogicalVectorView<'a>) -> Self {
                v.$name()
            }
        }
    };
}

impl_into_view! { Int, into_int_view }
impl_into_view! { Real, into_real_view }
impl_into_view! { Decimal, into_decimal_view }
impl_into_view! { Bytes, into_bytes_view }
impl_into_view! { DateTime, into_date_time_view }
impl_into_view! { Duration, into_duration_view }
impl_into_view! { Json, into_json_view }

/// A view to a concrete logical vector.
#[derive(Copy, Clone)]
pub enum ConcreteLogicalVectorView<'a, T> {
    Vector {
        physical_value: &'a [Option<T>],
        logical_rows: &'a [usize],
    },
    Scalar(&'a Option<T>),
}

impl<'a, T> Index<usize> for ConcreteLogicalVectorView<'a, T> {
    type Output = Option<T>;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        match self {
            ConcreteLogicalVectorView::Vector {
                physical_value,
                logical_rows,
            } => &physical_value[logical_rows[index]],
            ConcreteLogicalVectorView::Scalar(v) => v,
        }
    }
}

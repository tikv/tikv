// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate provides aggregate functions for batch executors.

#![allow(incomplete_features)]
#![feature(proc_macro_hygiene)]
#![feature(specialization)]

#[macro_use(box_try)]
extern crate tikv_util;

#[macro_use(other_err)]
extern crate tidb_query_common;

mod impl_avg;
mod impl_bit_op;
mod impl_count;
mod impl_first;
mod impl_max_min;
mod impl_sum;
mod impl_variance;
mod parser;
mod summable;
mod util;

use tidb_query_common::Result;
use tidb_query_datatype::{codec::data_type::*, expr::EvalContext};

pub use self::parser::{AggrDefinitionParser, AllAggrDefinitionParser};

/// A trait for all single parameter aggregate functions.
///
/// Unlike ordinary function, aggregate function calculates a summary value over multiple rows. To
/// save memory, this functionality is provided via an incremental update model:
///
/// 1. Each aggregate function associates a state structure, storing partially computed aggregate
///    results.
///
/// 2. The caller calls `update()` or `update_vector()` for each row to update the state.
///
/// 3. The caller finally calls `push_result()` to aggregate a summary value and push it into the
///    given data container.
///
/// This trait can be auto derived by using `tidb_query_codegen::AggrFunction`.
pub trait AggrFunction: std::fmt::Debug + Send + 'static {
    /// The display name of the function.
    fn name(&self) -> &'static str;

    /// Creates a new state instance. Different states aggregate independently.
    fn create_state(&self) -> Box<dyn AggrFunctionState>;
}

/// A trait for all single parameter aggregate function states.
///
/// Aggregate function states are created by corresponding aggregate functions. For each state,
/// it can be updated or aggregated (to finalize a result) independently.
///
/// Note that aggregate function states are strongly typed, that is, the caller must provide the
/// parameter in the correct data type for an aggregate function states that calculates over this
/// data type. To be safely boxed and placed in a vector, interfaces are provided in a form that
/// accept all kinds of data type. However, unmatched types will result in panics in runtime.
pub trait AggrFunctionState:
    std::fmt::Debug
    + Send
    + 'static
    + AggrFunctionStateUpdatePartial<&'static Int>
    + AggrFunctionStateUpdatePartial<&'static Real>
    + AggrFunctionStateUpdatePartial<&'static Decimal>
    + AggrFunctionStateUpdatePartial<BytesRef<'static>>
    + AggrFunctionStateUpdatePartial<&'static DateTime>
    + AggrFunctionStateUpdatePartial<&'static Duration>
    + AggrFunctionStateUpdatePartial<JsonRef<'static>>
    + AggrFunctionStateUpdatePartial<EnumRef<'static>>
    + AggrFunctionStateUpdatePartial<SetRef<'static>>
{
    // TODO: A better implementation is to specialize different push result targets. However
    // current aggregation executor cannot utilize it.
    fn push_result(&self, ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()>;
}

/// A helper trait for single parameter aggregate function states that only work over concrete eval
/// types. This is the actual and only trait that normal aggregate function states will implement.
///
/// Unlike `AggrFunctionState`, this trait only provides specialized `update()` and `push_result()`
/// functions according to the associated type. `update()` and `push_result()` functions that accept
/// any eval types (but will panic when eval type does not match expectation) will be generated via
/// implementations over this trait.
pub trait ConcreteAggrFunctionState: std::fmt::Debug + Send + 'static {
    type ParameterType: EvaluableRef<'static>;

    /// # Safety
    ///
    /// This function should be called with `update_concrete` macro.
    unsafe fn update_concrete_unsafe(
        &mut self,
        ctx: &mut EvalContext,
        value: Option<Self::ParameterType>,
    ) -> Result<()>;

    fn push_result(&self, ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()>;
}

#[macro_export]
macro_rules! update_concrete {
    ( $state:expr, $ctx:expr, $value:expr ) => {
        unsafe { $state.update_concrete_unsafe($ctx, $value.unsafe_into()) }
    };
}

#[macro_export]
macro_rules! update_vector {
    ( $state:expr, $ctx:expr, $physical_values:expr, $logical_rows:expr ) => {
        unsafe {
            $state.update_vector_unsafe(
                $ctx,
                $physical_values.phantom_data().unsafe_into(),
                $physical_values.unsafe_into(),
                $logical_rows,
            )
        }
    };
}

#[macro_export]
macro_rules! update_repeat {
    ( $state:expr, $ctx:expr, $value:expr, $repeat_times:expr ) => {
        unsafe { $state.update_repeat_unsafe($ctx, $value.unsafe_into(), $repeat_times) }
    };
}

#[macro_export]
macro_rules! update {
    ( $state:expr, $ctx:expr, $value:expr ) => {
        unsafe { $state.update_unsafe($ctx, $value.unsafe_into()) }
    };
}

#[macro_export]
macro_rules! impl_state_update_partial {
    ( $ty:tt ) => {
        #[inline]
        unsafe fn update_unsafe(
            &mut self,
            ctx: &mut EvalContext,
            value: Option<$ty>,
        ) -> Result<()> {
            self.update(ctx, value)
        }

        #[inline]
        unsafe fn update_repeat_unsafe(
            &mut self,
            ctx: &mut EvalContext,
            value: Option<$ty>,
            repeat_times: usize,
        ) -> Result<()> {
            self.update_repeat(ctx, value, repeat_times)
        }

        #[inline]
        unsafe fn update_vector_unsafe(
            &mut self,
            ctx: &mut EvalContext,
            phantom_data: Option<$ty>,
            physical_values: $ty::ChunkedType,
            logical_rows: &[usize],
        ) -> Result<()> {
            self.update_vector(ctx, phantom_data, physical_values, logical_rows)
        }
    };
}

#[macro_export]
macro_rules! impl_concrete_state {
    ( $ty:ty ) => {
        #[inline]
        unsafe fn update_concrete_unsafe(
            &mut self,
            ctx: &mut EvalContext,
            value: Option<$ty>,
        ) -> Result<()> {
            self.update_concrete(ctx, value)
        }
    };
}

#[macro_export]
macro_rules! impl_unmatched_function_state {
    ( $ty:ty ) => {
        impl<T1, T> super::AggrFunctionStateUpdatePartial<T1> for $ty
        where
            T1: EvaluableRef<'static> + 'static,
            T: EvaluableRef<'static> + 'static,
            VectorValue: VectorValueExt<T::EvaluableType>,
        {
            #[inline]
            default unsafe fn update_unsafe(
                &mut self,
                _ctx: &mut EvalContext,
                _value: Option<T1>,
            ) -> Result<()> {
                panic!("Unmatched parameter type")
            }

            #[inline]
            default unsafe fn update_repeat_unsafe(
                &mut self,
                _ctx: &mut EvalContext,
                _value: Option<T1>,
                _repeat_times: usize,
            ) -> Result<()> {
                panic!("Unmatched parameter type")
            }

            #[inline]
            default unsafe fn update_vector_unsafe(
                &mut self,
                _ctx: &mut EvalContext,
                _phantom_data: Option<T1>,
                _physical_values: T1::ChunkedType,
                _logical_rows: &[usize],
            ) -> Result<()> {
                panic!("Unmatched parameter type")
            }
        }
    };
}

/// A helper trait that provides `update()` and `update_vector()` over a concrete type, which will
/// be relied in `AggrFunctionState`.
pub trait AggrFunctionStateUpdatePartial<TT: EvaluableRef<'static>> {
    /// Updates the internal state giving one row data.
    ///
    /// # Panics
    ///
    /// Panics if the aggregate function does not support the supplied concrete data type as its
    /// parameter.
    ///
    /// # Safety
    ///
    /// This function should be called with `update` macro.
    unsafe fn update_unsafe(&mut self, ctx: &mut EvalContext, value: Option<TT>) -> Result<()>;

    /// Repeatedly updates the internal state giving one row data.
    ///
    /// # Panics
    ///
    /// Panics if the aggregate function does not support the supplied concrete data type as its
    /// parameter.
    ///
    /// # Safety
    ///
    /// This function should be called with `update_repeat_unsafe` macro.
    unsafe fn update_repeat_unsafe(
        &mut self,
        ctx: &mut EvalContext,
        value: Option<TT>,
        repeat_times: usize,
    ) -> Result<()>;

    /// Updates the internal state giving multiple rows data.
    ///
    /// # Panics
    ///
    /// Panics if the aggregate function does not support the supplied concrete data type as its
    /// parameter.
    ///
    /// # Safety
    ///
    /// This function should be called with `update_vector` macro.
    unsafe fn update_vector_unsafe(
        &mut self,
        ctx: &mut EvalContext,
        phantom_data: Option<TT>,
        physical_values: TT::ChunkedType,
        logical_rows: &[usize],
    ) -> Result<()>;
}

impl<T: EvaluableRef<'static>, State> AggrFunctionStateUpdatePartial<T> for State
where
    State: ConcreteAggrFunctionState,
{
    // All `ConcreteAggrFunctionState` implement `AggrFunctionStateUpdatePartial<T>`, which is
    // one of the trait bound that `AggrFunctionState` requires.

    #[inline]
    default unsafe fn update_unsafe(
        &mut self,
        _ctx: &mut EvalContext,
        _value: Option<T>,
    ) -> Result<()> {
        panic!("Unmatched parameter type")
    }

    #[inline]
    default unsafe fn update_repeat_unsafe(
        &mut self,
        _ctx: &mut EvalContext,
        _value: Option<T>,
        _repeat_times: usize,
    ) -> Result<()> {
        panic!("Unmatched parameter type")
    }

    #[inline]
    default unsafe fn update_vector_unsafe(
        &mut self,
        _ctx: &mut EvalContext,
        _phantom_data: Option<T>,
        _physical_values: T::ChunkedType,
        _logical_rows: &[usize],
    ) -> Result<()> {
        panic!("Unmatched parameter type")
    }
}

impl<T: EvaluableRef<'static>, State> AggrFunctionStateUpdatePartial<T> for State
where
    State: ConcreteAggrFunctionState<ParameterType = T>,
{
    #[inline]
    unsafe fn update_unsafe(&mut self, ctx: &mut EvalContext, value: Option<T>) -> Result<()> {
        self.update_concrete_unsafe(ctx, value)
    }

    #[inline]
    unsafe fn update_repeat_unsafe(
        &mut self,
        ctx: &mut EvalContext,
        value: Option<T>,
        repeat_times: usize,
    ) -> Result<()> {
        for _ in 0..repeat_times {
            self.update_concrete_unsafe(ctx, value.clone())?;
        }
        Ok(())
    }

    #[inline]
    unsafe fn update_vector_unsafe(
        &mut self,
        ctx: &mut EvalContext,
        _phantom_data: Option<T>,
        physical_values: T::ChunkedType,
        logical_rows: &[usize],
    ) -> Result<()> {
        for physical_index in logical_rows {
            self.update_concrete_unsafe(ctx, physical_values.get_option_ref(*physical_index))?;
        }
        Ok(())
    }
}

impl<F> AggrFunctionState for F
where
    F: ConcreteAggrFunctionState,
{
    fn push_result(&self, ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        <Self as ConcreteAggrFunctionState>::push_result(self, ctx, target)
    }
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::EvalType;

    use super::*;

    #[test]
    fn test_type_match() {
        /// A state that accepts Int and outputs Real.
        #[derive(Clone, Debug)]
        struct AggrFnStateFoo {
            sum: i64,
        }

        impl AggrFnStateFoo {
            fn new() -> Self {
                Self { sum: 0 }
            }
        }

        impl ConcreteAggrFunctionState for AggrFnStateFoo {
            type ParameterType = &'static Int;

            unsafe fn update_concrete_unsafe(
                &mut self,
                _ctx: &mut EvalContext,
                value: Option<&'static Int>,
            ) -> Result<()> {
                if let Some(v) = value {
                    self.sum += *v;
                }
                Ok(())
            }

            fn push_result(
                &self,
                _ctx: &mut EvalContext,
                target: &mut [VectorValue],
            ) -> Result<()> {
                target[0].push_real(Real::new(self.sum as f64).ok());
                Ok(())
            }
        }

        let mut ctx = EvalContext::default();
        let mut s = AggrFnStateFoo::new();

        // Update using `Int` should success.
        assert!(
            update!(
                &mut s as &mut dyn AggrFunctionStateUpdatePartial<_>,
                &mut ctx,
                Some(&1)
            )
            .is_ok()
        );
        assert!(
            update!(
                &mut s as &mut dyn AggrFunctionStateUpdatePartial<_>,
                &mut ctx,
                Some(&3)
            )
            .is_ok()
        );

        // Update using other data type should panic.
        let result = panic_hook::recover_safe(|| {
            let mut s = s.clone();
            let _ = update!(
                &mut s as &mut dyn AggrFunctionStateUpdatePartial<_>,
                &mut ctx,
                Real::new(1.0).ok().as_ref()
            );
        });
        assert!(result.is_err());

        let result = panic_hook::recover_safe(|| {
            let mut s = s.clone();
            let _ = update!(
                &mut s as &mut dyn AggrFunctionStateUpdatePartial<_>,
                &mut ctx,
                Some(&[1u8] as BytesRef<'_>)
            );
        });
        assert!(result.is_err());

        // Push result to Real VectorValue should success.
        let mut target = vec![VectorValue::with_capacity(0, EvalType::Real)];

        assert!(
            (&mut s as &mut dyn AggrFunctionState)
                .push_result(&mut ctx, &mut target)
                .is_ok()
        );
        assert_eq!(target[0].to_real_vec(), &[Real::new(4.0).ok()]);

        // Calling push result multiple times should also success.
        assert!(
            update!(
                &mut s as &mut dyn AggrFunctionStateUpdatePartial<_>,
                &mut ctx,
                Some(&1)
            )
            .is_ok()
        );
        assert!(
            (&mut s as &mut dyn AggrFunctionState)
                .push_result(&mut ctx, &mut target)
                .is_ok()
        );
        assert_eq!(
            target[0].to_real_vec(),
            &[Real::new(4.0).ok(), Real::new(5.0).ok()]
        );

        // Push result into other VectorValue should panic.
        let result = panic_hook::recover_safe(|| {
            let mut s = s.clone();
            let mut target: Vec<VectorValue> = Vec::new();
            let _ = (&mut s as &mut dyn AggrFunctionState).push_result(&mut ctx, &mut target[..]);
        });
        assert!(result.is_err());

        let result = panic_hook::recover_safe(|| {
            let mut s = s.clone();
            let mut target: Vec<VectorValue> = vec![VectorValue::with_capacity(0, EvalType::Int)];
            let _ = (&mut s as &mut dyn AggrFunctionState).push_result(&mut ctx, &mut target[..]);
        });
        assert!(result.is_err());
    }
}

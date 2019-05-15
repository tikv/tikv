// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This module provides aggregate functions for batch executors.

mod impl_avg;
mod impl_count;
mod impl_sum;
mod parser;
mod summable;
mod util;

pub use self::parser::AggrDefinitionParser;

use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

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
/// This trait can be auto derived by using `cop_codegen::AggrFunction`.
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
    + AggrFunctionStateUpdatePartial<Int>
    + AggrFunctionStateUpdatePartial<Real>
    + AggrFunctionStateUpdatePartial<Decimal>
    + AggrFunctionStateUpdatePartial<Bytes>
    + AggrFunctionStateUpdatePartial<DateTime>
    + AggrFunctionStateUpdatePartial<Duration>
    + AggrFunctionStateUpdatePartial<Json>
    + AggrFunctionStateResultPartial<Vec<Option<Int>>>
    + AggrFunctionStateResultPartial<Vec<Option<Real>>>
    + AggrFunctionStateResultPartial<Vec<Option<Decimal>>>
    + AggrFunctionStateResultPartial<Vec<Option<Bytes>>>
    + AggrFunctionStateResultPartial<Vec<Option<DateTime>>>
    + AggrFunctionStateResultPartial<Vec<Option<Duration>>>
    + AggrFunctionStateResultPartial<Vec<Option<Json>>>
    + AggrFunctionStateResultPartial<[VectorValue]>
{
}

/// A helper trait for single parameter aggregate function states that only work over concrete eval
/// types. This is the actual and only trait that normal aggregate function states will implement.
///
/// Unlike `AggrFunctionState`, this trait only provides specialized `update()` and `push_result()`
/// functions according to the associated type. `update()` and `push_result()` functions that accept
/// any eval types (but will panic when eval type does not match expectation) will be generated via
/// implementations over this trait.
pub trait ConcreteAggrFunctionState: std::fmt::Debug + Send + 'static {
    type ParameterType: Evaluable;
    type ResultTargetType: AggrResultAppendable + ?Sized;

    fn update_concrete(
        &mut self,
        ctx: &mut EvalContext,
        value: &Option<Self::ParameterType>,
    ) -> Result<()>;

    fn push_result_concrete(
        &self,
        ctx: &mut EvalContext,
        target: &mut Self::ResultTargetType,
    ) -> Result<()>;
}

/// A helper trait that provides `update()` and `update_vector()` over a concrete type, which will
/// be relied in `AggrFunctionState`.
pub trait AggrFunctionStateUpdatePartial<T: Evaluable> {
    /// Updates the internal state giving one row data.
    ///
    /// # Panics
    ///
    /// Panics if the aggregate function does not support the supplied concrete data type as its
    /// parameter.
    fn update(&mut self, ctx: &mut EvalContext, value: &Option<T>) -> Result<()>;

    /// Repeatedly updates the internal state giving one row data.
    ///
    /// # Panics
    ///
    /// Panics if the aggregate function does not support the supplied concrete data type as its
    /// parameter.
    fn update_repeat(
        &mut self,
        ctx: &mut EvalContext,
        value: &Option<T>,
        repeat_times: usize,
    ) -> Result<()>;

    /// Updates the internal state giving multiple rows data.
    ///
    /// # Panics
    ///
    /// Panics if the aggregate function does not support the supplied concrete data type as its
    /// parameter.
    fn update_vector(&mut self, ctx: &mut EvalContext, values: &[Option<T>]) -> Result<()>;
}

/// A trait for types that can be used as an append target container for the aggregation result.
pub trait AggrResultAppendable {}

impl<T: Evaluable> AggrResultAppendable for Vec<Option<T>> {
    // The aggregate result can be appended to a simple strongly typed data vector.
}

impl AggrResultAppendable for [VectorValue] {
    // The aggregate result can also be appended into a multi valued and dynamic typed container.
}

/// A helper trait that provides `push_result()` over a concrete type, which will be relied in
/// `AggrFunctionState`.
pub trait AggrFunctionStateResultPartial<T: AggrResultAppendable + ?Sized> {
    /// Finalizes and calculates the aggregated summary value.
    ///
    /// This function is idempotent, i.e. safe to be called multiple times.
    ///
    /// # Panics
    ///
    /// Panics if the aggregate function does not support the supplied concrete data type as its
    /// return value.
    fn push_result(&self, ctx: &mut EvalContext, target: &mut T) -> Result<()>;
}

impl<T: Evaluable, State> AggrFunctionStateUpdatePartial<T> for State
where
    State: ConcreteAggrFunctionState,
{
    // All `ConcreteAggrFunctionState` implement `AggrFunctionStateUpdatePartial<T>`, which is
    // one of the trait bound that `AggrFunctionState` requires.

    #[inline]
    default fn update(&mut self, _ctx: &mut EvalContext, _value: &Option<T>) -> Result<()> {
        panic!("Unmatched parameter type")
    }

    #[inline]
    default fn update_repeat(
        &mut self,
        _ctx: &mut EvalContext,
        _value: &Option<T>,
        _repeat_times: usize,
    ) -> Result<()> {
        panic!("Unmatched parameter type")
    }

    #[inline]
    default fn update_vector(
        &mut self,
        _ctx: &mut EvalContext,
        _values: &[Option<T>],
    ) -> Result<()> {
        panic!("Unmatched parameter type")
    }
}

impl<T: Evaluable, State> AggrFunctionStateUpdatePartial<T> for State
where
    State: ConcreteAggrFunctionState<ParameterType = T>,
{
    #[inline]
    fn update(&mut self, ctx: &mut EvalContext, value: &Option<T>) -> Result<()> {
        self.update_concrete(ctx, value)
    }

    #[inline]
    fn update_repeat(
        &mut self,
        ctx: &mut EvalContext,
        value: &Option<T>,
        repeat_times: usize,
    ) -> Result<()> {
        for _ in 0..repeat_times {
            self.update_concrete(ctx, value)?;
        }
        Ok(())
    }

    #[inline]
    fn update_vector(&mut self, ctx: &mut EvalContext, values: &[Option<T>]) -> Result<()> {
        for value in values {
            self.update_concrete(ctx, value)?;
        }
        Ok(())
    }
}

impl<T: AggrResultAppendable + ?Sized, State> AggrFunctionStateResultPartial<T> for State
where
    State: ConcreteAggrFunctionState,
{
    // All `ConcreteAggrFunctionState` implement `AggrFunctionStateResultPartial<T>`, which is
    // one of the trait bound that `AggrFunctionState` requires.

    #[inline]
    default fn push_result(&self, _ctx: &mut EvalContext, _target: &mut T) -> Result<()> {
        panic!("Unmatched result append target type")
    }
}

impl<T: AggrResultAppendable + ?Sized, State> AggrFunctionStateResultPartial<T> for State
where
    State: ConcreteAggrFunctionState<ResultTargetType = T>,
{
    #[inline]
    fn push_result(&self, ctx: &mut EvalContext, target: &mut T) -> Result<()> {
        self.push_result_concrete(ctx, target)
    }
}

impl<F> AggrFunctionState for F
where
    F: ConcreteAggrFunctionState,
{
    // All `ConcreteAggrFunctionState` can implement `AggrFunctionState` now, since they meet
    // all trait bound of `AggrFunctionState`.
}

#[cfg(test)]
mod tests {
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
            type ParameterType = Int;
            type ResultTargetType = Vec<Option<Real>>;

            fn update_concrete(
                &mut self,
                _ctx: &mut EvalContext,
                value: &Option<Int>,
            ) -> Result<()> {
                if let Some(v) = value {
                    self.sum += *v;
                }
                Ok(())
            }

            fn push_result_concrete(
                &self,
                _ctx: &mut EvalContext,
                target: &mut Vec<Option<Real>>,
            ) -> Result<()> {
                target.push(Real::new(self.sum as f64).ok());
                Ok(())
            }
        }

        let mut ctx = EvalContext::default();
        let mut s = AggrFnStateFoo::new();

        // Update using `Int` should success.
        assert!((&mut s as &mut dyn AggrFunctionStateUpdatePartial<_>)
            .update(&mut ctx, &Some(1))
            .is_ok());
        assert!((&mut s as &mut dyn AggrFunctionStateUpdatePartial<_>)
            .update(&mut ctx, &Some(3))
            .is_ok());

        // Update using other data type should panic.
        let result = panic_hook::recover_safe(|| {
            let mut s = s.clone();
            let _ = (&mut s as &mut dyn AggrFunctionStateUpdatePartial<_>)
                .update(&mut ctx, &Real::new(1.0).ok());
        });
        assert!(result.is_err());

        let result = panic_hook::recover_safe(|| {
            let mut s = s.clone();
            let _ = (&mut s as &mut dyn AggrFunctionStateUpdatePartial<_>)
                .update(&mut ctx, &Some(vec![1u8]));
        });
        assert!(result.is_err());

        // Push result into `Vec<Option<Real>>` should success.
        let mut target: Vec<Option<Real>> = Vec::new();

        assert!((&mut s as &mut dyn AggrFunctionStateResultPartial<_>)
            .push_result(&mut ctx, &mut target)
            .is_ok());
        assert_eq!(target, vec![Real::new(4.0).ok()]);

        // Calling push result multiple times should also success.
        assert!((&mut s as &mut dyn AggrFunctionStateUpdatePartial<_>)
            .update(&mut ctx, &Some(1))
            .is_ok());
        assert!((&mut s as &mut dyn AggrFunctionStateResultPartial<_>)
            .push_result(&mut ctx, &mut target)
            .is_ok());
        assert_eq!(target, vec![Real::new(4.0).ok(), Real::new(5.0).ok()]);

        // Push result into other data type should panic.
        let result = panic_hook::recover_safe(|| {
            let mut s = s.clone();
            let mut target: Vec<Option<Int>> = Vec::new();
            let _ = (&mut s as &mut dyn AggrFunctionStateResultPartial<_>)
                .push_result(&mut ctx, &mut target);
        });
        assert!(result.is_err());

        let result = panic_hook::recover_safe(|| {
            let mut s = s.clone();
            let mut target: Vec<Option<Decimal>> = Vec::new();
            let _ = (&mut s as &mut dyn AggrFunctionStateResultPartial<_>)
                .push_result(&mut ctx, &mut target);
        });
        assert!(result.is_err());

        let result = panic_hook::recover_safe(|| {
            let mut s = s.clone();
            let mut target: Vec<VectorValue> = Vec::new();
            let _ = (&mut s as &mut dyn AggrFunctionStateResultPartial<_>)
                .push_result(&mut ctx, &mut target[..]);
        });
        assert!(result.is_err());
    }
}

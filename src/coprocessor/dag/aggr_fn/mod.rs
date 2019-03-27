// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! This module provides aggregate functions for batch executors.

use cop_datatype::EvalType;

use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::Result;

/// A trait for all single parameter aggregate functions.
///
/// Unlike ordinary function, aggregate function calculates a summary value over multiple rows. To
/// save memory, this functionality is provided via an incremental update model:
///
/// 1. Each aggregate function instance maintains an internal state, storing partially computed
/// aggregate results.
///
/// 2. The caller calls `update()` or `update_vector()` for each row to update the internal state.
///
/// 3. The caller finally calls `result()` to finalize and calculate the aggregated summary value.
///
/// Note that aggregate functions are strongly typed, that is, the caller must provide the parameter
/// in the correct data type for an aggregate function that calculates over this data type.
/// Unmatched types will result in panics.
pub trait AggrFunction:
    std::fmt::Debug
    + Send
    + 'static
    + AggrFunctionUpdatePartial<Option<Int>>
    + AggrFunctionUpdatePartial<Option<Real>>
    + AggrFunctionUpdatePartial<Option<Decimal>>
    + AggrFunctionUpdatePartial<Option<Bytes>>
    + AggrFunctionUpdatePartial<Option<DateTime>>
    + AggrFunctionUpdatePartial<Option<Duration>>
    + AggrFunctionUpdatePartial<Option<Json>>
    + AggrFunctionResultPartial<Option<Int>>
    + AggrFunctionResultPartial<Option<Real>>
    + AggrFunctionResultPartial<Option<Decimal>>
    + AggrFunctionResultPartial<Option<Bytes>>
    + AggrFunctionResultPartial<Option<DateTime>>
    + AggrFunctionResultPartial<Option<Duration>>
    + AggrFunctionResultPartial<Option<Json>>
{
    fn parameter_type(&self) -> EvalType;

    fn return_type(&self) -> EvalType;
}

/// A helper trait for single parameter aggregate functions that only work over concrete eval
/// types. This is the actual and only trait that normal aggregate function will implement.
///
/// Unlike `AggrFunction`, this trait only provides specialized `update()` and `result()` functions
/// according to the associated type. `update()` and `result()` functions that accept any eval
/// types (but will panic when eval type does not match expectation) will be generated via
/// implementations over this trait.
pub trait ConcreteAggrFunction: std::fmt::Debug + Send + 'static {
    type ParameterType: Evaluable;
    type ReturnType: Evaluable;

    /// Updates the internal state giving one row data.
    fn update_concrete(&mut self, value: &Self::ParameterType) -> Result<()>;

    /// Finalizes and calculates the aggregated summary value.
    ///
    /// This function is idempotent, i.e. safe to be called multiple times.
    fn result_concrete(&self) -> Result<Self::ReturnType>;
}

/// A helper trait that provides `update()` and `update_vector()` over a concrete type, which will
/// be relied in `AggrFunction`.
pub trait AggrFunctionUpdatePartial<T: Evaluable> {
    /// Updates the internal state giving one row data.
    ///
    /// # Panics
    ///
    /// Panics if the aggregate function does not support the supplied concrete data type as its
    /// parameter.
    fn update(&mut self, value: &T) -> Result<()>;

    /// Updates the internal state giving multiple rows data.
    ///
    /// # Panics
    ///
    /// Panics if the aggregate function does not support the supplied concrete data type as its
    /// parameter.
    fn update_vector(&mut self, values: &[T]) -> Result<()>;
}

/// A helper trait that provides `result()` over a concrete type, which will be relied in
/// `AggrFunction`.
pub trait AggrFunctionResultPartial<T: Evaluable> {
    /// Finalizes and calculates the aggregated summary value.
    ///
    /// This function is idempotent, i.e. safe to be called multiple times.
    ///
    /// # Panics
    ///
    /// Panics if the aggregate function does not support the supplied concrete data type as its
    /// return value.
    fn result(&self) -> Result<T>;
}

impl<T: Evaluable, F> AggrFunctionUpdatePartial<T> for F
where
    F: ConcreteAggrFunction,
{
    // For **all** `ConcreteAggrFunction` structures, they also implement
    // `AggrFunctionUpdatePartial<T>`. The implementation here is a default behaviour, which is,
    // always panic.
    //
    // For example, now we will have:
    //
    // `<SomeAggrFn as AggrFunctionUpdatePartial<i64>>::update()` will be `panic!()`
    // `<SomeAggrFn as AggrFunctionUpdatePartial<f64>>::update()` will be `panic!()`
    // ...
    //
    // Later, we will provide an override (more specific) implementation (i.e. specialization) that:
    //
    // If `SomeAggrFn` implements `ConcreteAggrFunction<ParameterType=i64>`, then
    // `<SomeAggrFn as AggrFunctionUpdatePartial<i64>>::update()` will be
    //   `<SomeAggrFn as ConcreteAggrFunction<ParameterType=i64>>::update_concrete()`
    // If `SomeAggrFn` implements `...<...=f64>`, then ...

    #[inline(always)]
    default fn update(&mut self, _value: &T) -> Result<()> {
        panic!("Unmatched parameter type")
    }

    #[inline(always)]
    default fn update_vector(&mut self, _values: &[T]) -> Result<()> {
        panic!("Unmatched parameter type")
    }
}

impl<T: Evaluable, F> AggrFunctionUpdatePartial<T> for F
where
    F: ConcreteAggrFunction<ParameterType = T>,
{
    // This is the more specific implementation, only for the case that
    // `T` inside `ConcreteAggrFunction<ParameterType=T>` matches
    // `T` inside `AggrFunctionUpdatePartial<T>`.

    #[inline(always)]
    fn update(&mut self, value: &T) -> Result<()> {
        self.update_concrete(value)
    }

    #[inline(always)]
    fn update_vector(&mut self, values: &[T]) -> Result<()> {
        for value in values {
            self.update_concrete(value)?;
        }
        Ok(())
    }
}

impl<T: Evaluable, F: ConcreteAggrFunction> AggrFunctionResultPartial<T> for F {
    #[inline(always)]
    default fn result(&self) -> Result<T> {
        panic!("Unmatched return type")
    }
}

impl<T: Evaluable, F: ConcreteAggrFunction<ReturnType = T>> AggrFunctionResultPartial<T> for F {
    #[inline(always)]
    fn result(&self) -> Result<T> {
        self.result_concrete()
    }
}

impl<F> AggrFunction for F
where
    F: ConcreteAggrFunction,
{
    // For all `ConcreteAggrFunction` structures, we can directly provide its parameter type
    // or return type according to the type information carried in `ConcreteAggrFunction`'s
    // associate types.

    fn parameter_type(&self) -> EvalType {
        F::ParameterType::eval_type()
    }

    fn return_type(&self) -> EvalType {
        F::ReturnType::eval_type()
    }
}

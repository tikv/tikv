// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Procedural macros used in the tidb_query component; part of the coprocessor
//! subsystem.
//!
//! For an overview of the coprocessor architecture, see the documentation on
//! [tikv/src/coprocessor](https://github.com/tikv/tikv/blob/master/src/coprocessor/mod.rs).
//!
//! This crate exports a custom derive for [`AggrFunction`](https://github.com/tikv/tikv/blob/master/components/tidb_query_vec_aggr/src/mod.rs)
//! and an attribute macro called `rpn_fn` for use on functions which provide
//! coprocessor functionality. `rpn_fn` is documented in the [rpn_function](rpn_function.rs)
//! module.

#![feature(proc_macro_diagnostic)]
#![recursion_limit = "256"]

extern crate proc_macro;

mod aggr_function;
mod rpn_function;

use darling::FromDeriveInput;
use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

/// The `AggrFunction` custom derive.
#[proc_macro_derive(AggrFunction, attributes(aggr_function))]
pub fn aggr_function_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let r = aggr_function::AggrFunctionOpts::from_derive_input(&input);
    match r {
        Err(e) => panic!("{}", e),
        Ok(r) => TokenStream::from(r.generate_tokens()),
    }
}

/// The `rpn_fn` attribute.
#[proc_macro_attribute]
pub fn rpn_fn(attr: TokenStream, input: TokenStream) -> TokenStream {
    match rpn_function::transform(attr.into(), input.into()) {
        Ok(tokens) => TokenStream::from(tokens),
        Err(e) => TokenStream::from(e.to_compile_error()),
    }
}

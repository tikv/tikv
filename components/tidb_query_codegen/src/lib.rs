// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(proc_macro_diagnostic)]
#![recursion_limit = "256"]

extern crate proc_macro;

mod aggr_function;
mod rpn_function;

use darling::FromDeriveInput;
use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(AggrFunction, attributes(aggr_function))]
pub fn aggr_function_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let r = aggr_function::AggrFunctionOpts::from_derive_input(&input);
    match r {
        Err(e) => panic!("{}", e),
        Ok(r) => TokenStream::from(r.generate_tokens()),
    }
}

#[proc_macro_attribute]
pub fn rpn_fn(attr: TokenStream, input: TokenStream) -> TokenStream {
    match rpn_function::transform(attr.into(), input.into()) {
        Ok(tokens) => TokenStream::from(tokens),
        Err(e) => TokenStream::from(e.to_compile_error()),
    }
}

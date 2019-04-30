// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "128"]

#[macro_use]
extern crate darling;
#[macro_use]
extern crate quote;
#[macro_use]
extern crate syn;
extern crate proc_macro;

mod aggr_function;
mod rpn_function;

use darling::FromDeriveInput;
use proc_macro::TokenStream;
use syn::DeriveInput;

#[proc_macro_derive(RpnFunction, attributes(rpn_function))]
pub fn rpn_function_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let r = rpn_function::RpnFunctionOpts::from_derive_input(&input);
    match r {
        Err(e) => panic!("{}", e),
        Ok(r) => TokenStream::from(r.generate_tokens()),
    }
}

#[proc_macro_derive(AggrFunction, attributes(aggr_function))]
pub fn aggr_function_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let r = aggr_function::AggrFunctionOpts::from_derive_input(&input);
    match r {
        Err(e) => panic!("{}", e),
        Ok(r) => TokenStream::from(r.generate_tokens()),
    }
}

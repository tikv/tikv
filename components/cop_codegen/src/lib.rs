// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(proc_macro_diagnostic)]
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

use self::rpn_function::RpnFnGenerator;

use darling::FromDeriveInput;
use proc_macro::{Diagnostic, TokenStream};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{DeriveInput, Ident, ItemFn};

type Result<T> = std::result::Result<T, Diagnostic>;

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

#[proc_macro_attribute]
pub fn rpn_fn(attr: TokenStream, input: TokenStream) -> TokenStream {
    let meta_parser = Punctuated::<Ident, Token![,]>::parse_terminated;
    let meta = meta_parser.parse(attr).unwrap().into_iter().collect();
    let item_fn = parse_macro_input!(input as ItemFn);

    let input = RpnFnGenerator::new(meta, item_fn);
    match input.map(RpnFnGenerator::generate) {
        Ok(tokens) => TokenStream::from(tokens),
        Err(e) => {
            e.emit();
            std::process::exit(1)
        }
    }
}

#![recursion_limit = "128"]

#[macro_use]
extern crate darling;
#[macro_use]
extern crate quote;
extern crate proc_macro;

mod rpn_function;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(RpnFunction, attributes(rpn_function))]
pub fn rpn_function_derive(input: TokenStream) -> TokenStream {
    use darling::FromDeriveInput;
    let input = parse_macro_input!(input as DeriveInput);
    let r = rpn_function::RpnFunctionOpts::from_derive_input(&input);
    match r {
        Err(e) => panic!("{}", e),
        Ok(r) => TokenStream::from(r.generate_tokens()),
    }
}

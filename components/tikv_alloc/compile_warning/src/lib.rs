#![feature(proc_macro_diagnostic)]

extern crate proc_macro;
use proc_macro::{Span, TokenStream};

#[proc_macro]
pub fn compile_warning(input: TokenStream) -> TokenStream {
    Span::call_site().warning(input.to_string()).emit();
    TokenStream::new()
}

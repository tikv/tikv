// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Procedural macros used for memory trace.

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Ident};

fn readable_name(name: &Ident) -> String {
    let mut name = name.to_string();
    name.make_ascii_lowercase();
    let mut res = String::with_capacity(name.len());
    for p in name.split('_') {
        res.push_str(p);
        res.push(' ');
    }
    res
}

#[proc_macro_derive(MemoryTraceHelper, attributes(name))]
pub fn memory_trace_provider_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let imp;

    match input.data {
        Data::Struct(ref s) => match s.fields {
            Fields::Named(ref fields) => {
                let total = fields.named.iter().map(|f| {
                    let name = &f.ident;
                    let id = readable_name(name.as_ref().unwrap());
                    quote! {
                        let s = self.#name.load(std::sync::atomic::Ordering::Relaxed);
                        sum += s;
                        sub_trace.add_sub_trace(#id).set_size(s);
                    }
                });
                let sum = quote! {
                    #(#total)*
                };
                let count = fields.named.len();
                imp = quote! {
                    let sub_trace = trace.add_sub_trace_with_capacity(id, #count);
                    let mut sum = 0;
                    #sum
                    sub_trace.set_size(sum);
                    sum
                };
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    };
    let expanded = quote! {
        impl #name {
            #[inline]
            pub fn trace(&self, id: impl Into<tikv_alloc::trace::Id>, trace: &mut tikv_alloc::trace::MemoryTrace) -> usize {
                #imp
            }
        }
    };
    TokenStream::from(expanded)
}

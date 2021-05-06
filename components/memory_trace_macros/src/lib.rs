// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Procedural macros used for memory trace.

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

#[proc_macro_derive(MemoryTraceHelper, attributes(name))]
pub fn memory_trace_reset_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let imp;

    match input.data {
        Data::Struct(ref s) => match s.fields {
            Fields::Named(ref fields) => {
                let total = fields.named.iter().map(|f| {
                    let name = &f.ident;
                    quote! {
                        lhs_sum += self.#name;
                        rhs_sum += rhs.#name;
                        self.#name = rhs.#name;
                    }
                });
                let sum = quote! {
                    #(#total)*
                };
                imp = quote! {
                    use tikv_alloc::trace::TraceEvent;
                    use std::cmp::Ordering;

                    let mut lhs_sum: usize = 0;
                    let mut rhs_sum: usize = 0;
                    #sum
                    match lhs_sum.cmp(&rhs_sum) {
                        Ordering::Greater => Some(TraceEvent::Sub(lhs_sum-rhs_sum)),
                        Ordering::Less => Some(TraceEvent::Add(rhs_sum-lhs_sum)),
                        Ordering::Equal => None,
                    }
                };
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    };
    let expanded = quote! {
        impl #name {
            #[inline]
            pub fn reset(&mut self, rhs: Self) -> Option<tikv_alloc::trace::TraceEvent> {
                #imp
            }
        }
    };
    TokenStream::from(expanded)
}

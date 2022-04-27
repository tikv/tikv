// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Procedural macros used for memory trace.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// MemoryTraceHelper adds two methods `reset` and `sum` to derived struct.
/// All fields of derived struct should be `usize`.
/// `reset` updates the struct and returns a delta represented by a `TraceEvent`
/// `sum` returns the summary of all field values.
#[proc_macro_derive(MemoryTraceHelper, attributes(name))]
pub fn memory_trace_reset_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let reset_imp;
    let sum_imp;

    match input.data {
        Data::Struct(ref s) => match s.fields {
            Fields::Named(ref fields) => {
                let reset_total = fields.named.iter().map(|f| {
                    let name = &f.ident;
                    quote! {
                        lhs_sum += self.#name;
                        rhs_sum += rhs.#name;
                        self.#name = rhs.#name;
                    }
                });
                reset_imp = quote! {
                    use tikv_alloc::trace::TraceEvent;
                    use std::cmp::Ordering;

                    let mut lhs_sum: usize = 0;
                    let mut rhs_sum: usize = 0;
                    #(#reset_total)*
                    match lhs_sum.cmp(&rhs_sum) {
                        Ordering::Greater => Some(TraceEvent::Sub(lhs_sum-rhs_sum)),
                        Ordering::Less => Some(TraceEvent::Add(rhs_sum-lhs_sum)),
                        Ordering::Equal => None,
                    }
                };

                let sum_total = fields.named.iter().map(|f| {
                    let name = &f.ident;
                    quote! {
                        sum += self.#name;
                    }
                });
                sum_imp = quote! {
                    let mut sum: usize = 0;
                    #(#sum_total)*
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
            pub fn reset(&mut self, rhs: Self) -> Option<tikv_alloc::trace::TraceEvent> {
                #reset_imp
            }

            #[inline]
            pub fn sum(&self) -> usize {
                #sum_imp
            }
        }
    };
    TokenStream::from(expanded)
}

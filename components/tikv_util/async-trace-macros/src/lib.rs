// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

// This code is based on the `attribute` module of `tokio-rs/async-backtrace`.
// Thanks for the great tokio project!
//
// Origin license (MIT):
// Copyright (c) 2022 Tokio Contributors
// Permission is hereby granted, free of charge, to any
// person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the
// Software without restriction, including without
// limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice
// shall be included in all copies or substantial portions
// of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
// ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
// TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
// SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use proc_macro2::TokenStream;
use quote::ToTokens;
use syn::{
    parse::{Parse, ParseStream},
    Attribute, Block, ItemFn, Signature, Visibility,
};

mod codegen;

#[proc_macro_attribute]
pub fn framed(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    assert!(args.is_empty());
    // Cloning a `TokenStream` is cheap since it's reference counted internally.
    instrument_precise(item.clone()).unwrap_or_else(|_err| instrument_speculative(item))
}

/// Instrument the function, without parsing the function body (instead using
/// the raw tokens).
fn instrument_speculative(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(item as MaybeItemFn);
    let instrumented_function_name = input.sig.ident.to_string();
    codegen::gen_function(input.as_ref(), instrumented_function_name.as_str(), None).into()
}

/// Instrument the function, by fully parsing the function body,
/// which allows us to rewrite some statements related to async-like patterns.
fn instrument_precise(
    item: proc_macro::TokenStream,
) -> Result<proc_macro::TokenStream, syn::Error> {
    let input = syn::parse::<ItemFn>(item)?;
    let instrumented_function_name = input.sig.ident.to_string();

    // check for async_trait-like patterns in the block, and instrument
    // the future instead of the wrapper
    if let Some(async_like) = codegen::AsyncInfo::from_fn(&input) {
        return Ok(async_like.gen_async(instrumented_function_name.as_str()));
    }

    Ok(codegen::gen_function((&input).into(), instrumented_function_name.as_str(), None).into())
}

/// This is a more flexible/imprecise `ItemFn` type,
/// which's block is just a `TokenStream` (it may contain invalid code).
#[derive(Debug, Clone)]
struct MaybeItemFn {
    attrs: Vec<Attribute>,
    vis: Visibility,
    sig: Signature,
    block: TokenStream,
}

impl MaybeItemFn {
    fn as_ref(&self) -> MaybeItemFnRef<'_, TokenStream> {
        MaybeItemFnRef {
            attrs: &self.attrs,
            vis: &self.vis,
            sig: &self.sig,
            block: &self.block,
        }
    }
}

/// This parses a `TokenStream` into a `MaybeItemFn`
/// (just like `ItemFn`, but skips parsing the body).
impl Parse for MaybeItemFn {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let attrs = input.call(syn::Attribute::parse_outer)?;
        let vis: Visibility = input.parse()?;
        let sig: Signature = input.parse()?;
        let block: TokenStream = input.parse()?;
        Ok(Self {
            attrs,
            vis,
            sig,
            block,
        })
    }
}

/// A generic reference type for `MaybeItemFn`,
/// that takes a generic block type `B` that implements `ToTokens` (eg.
/// `TokenStream`, `Block`).
#[derive(Debug, Clone)]
struct MaybeItemFnRef<'a, B: ToTokens> {
    attrs: &'a Vec<Attribute>,
    vis: &'a Visibility,
    sig: &'a Signature,
    block: &'a B,
}

impl<'a> From<&'a ItemFn> for MaybeItemFnRef<'a, Box<Block>> {
    fn from(val: &'a ItemFn) -> Self {
        MaybeItemFnRef {
            attrs: &val.attrs,
            vis: &val.vis,
            sig: &val.sig,
            block: &val.block,
        }
    }
}

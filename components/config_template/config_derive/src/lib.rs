// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate provides a macro that can be used to generate code to update
//! and diff a config
//!
//! For example, the following code
//!
//! ```ignore
//! #[derive(Config)]
//! struct Config {
//!     size: usize,
//!     #[not_support]
//!     count: u64,
//!     #[config(sub_module)]
//!     suv_cfg: SubConfig,
//! }
//! ```
//!
//! generates
//!
//! ```ignore
//!
//! ```
//!
//! Wildcard match arm is also supported (but there will be no substitution).

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::*;

#[proc_macro_derive(Configable, attributes(config))]
pub fn config(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    match generate_token(ast.ident, ast.data) {
        Ok(res) => res.into(),
        Err(e) => TokenStream::from(e.to_compile_error()),
    }
}

fn generate_token(name: Ident, data: Data) -> std::result::Result<proc_macro2::TokenStream, Error> {
    let fields = get_struct_fields(name.span(), &data)?;
    let update_fn = update(&fields)?;
    let diff_fn = diff(&fields)?;
    Ok(quote! {
        impl config_template::Configable for #name {
            #update_fn
            #diff_fn
        }
    })
}

fn get_struct_fields(
    span: proc_macro2::Span,
    data: &Data,
) -> std::result::Result<&Punctuated<Field, Comma>, Error> {
    if let Data::Struct(DataStruct {
        fields: Fields::Named(FieldsNamed { ref named, .. }),
        ..
    }) = data
    {
        Ok(named)
    } else {
        Err(Error::new(
            span,
            "expect derive Configable on struct with named fields!",
        ))
    }
}

fn update(fields: &Punctuated<Field, Comma>) -> Result<proc_macro2::TokenStream> {
    let incomming = Ident::new("incomming", proc_macro2::Span::call_site());
    let mut update_fields = Vec::with_capacity(fields.len());
    for field in fields {
        let (not_support, sub_module) = get_attrs(&field.attrs)?;
        if not_support || field.ident.is_none() {
            continue;
        }
        let name = field.ident.as_ref().unwrap();
        let name_lit = LitStr::new(&format!("{}", name), name.span());
        let f = if sub_module {
            quote! {
                if let Some(config_template::ConfigValue::Module(v)) = #incomming.remove(#name_lit) {
                    config_template::Configable::update(&mut self.#name, v);
                }
            }
        } else {
            quote! {
                if let Some(v) = #incomming.remove(#name_lit) {
                    self.#name = v.into();
                }
            }
        };
        update_fields.push(f);
    }
    Ok(quote! {
        fn update(&mut self, mut #incomming: config_template::PartialChange) {
            #(#update_fields)*
        }
    })
}

fn diff<P>(fields: &Punctuated<Field, P>) -> Result<proc_macro2::TokenStream> {
    let diff_ident = Ident::new("diff_ident", proc_macro2::Span::call_site());
    let incomming = Ident::new("incomming", proc_macro2::Span::call_site());
    let mut diff_fields = Vec::with_capacity(fields.len());
    for field in fields {
        let (not_support, sub_module) = get_attrs(&field.attrs)?;
        if not_support || field.ident.is_none() {
            continue;
        }
        let name = field.ident.as_ref().unwrap();
        let name_lit = LitStr::new(&format!("{}", name), name.span());
        let f = if sub_module {
            quote! {
                {
                    let diff = config_template::Configable::diff(&self.#name, #incomming.#name);
                    if diff.len() != 0 {
                        #diff_ident.insert(#name_lit.to_owned(), config_template::ConfigValue::from(diff));
                    }
                }
            }
        } else {
            quote! {
                if self.#name != #incomming.#name {
                    #diff_ident.insert(#name_lit.to_owned(), config_template::ConfigValue::from(#incomming.#name));
                }
            }
        };
        diff_fields.push(f);
    }
    Ok(quote! {
        fn diff(&self, mut #incomming: Self) -> config_template::PartialChange {
            let mut #diff_ident = std::collections::HashMap::default();
            #(#diff_fields)*
            #diff_ident
        }
    })
}

fn get_attrs(attrs: &Vec<Attribute>) -> Result<(bool, bool)> {
    let (mut not_support, mut sub_module) = (false, false);
    for attr in attrs.iter() {
        if !is_config_attr(&attr) {
            continue;
        }
        let name = attr.parse_args::<Ident>()?;
        if name == "not_support" {
            not_support = true;
        } else if name == "sub_module" {
            sub_module = true;
        } else {
            return Err(Error::new(
                name.span(),
                "expect #[config(not_support)] or #[config(sub_module)]",
            ));
        }
    }
    Ok((not_support, sub_module))
}

fn is_config_attr(attr: &Attribute) -> bool {
    for s in attr.path.segments.iter() {
        if s.ident == "config" {
            return true;
        }
    }
    false
}

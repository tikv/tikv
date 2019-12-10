// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate provides a macro that can be used to generate code to
//! implement `Configuration` trait

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::*;

#[proc_macro_derive(Configuration, attributes(config))]
pub fn config(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    match generate_token(ast.ident, ast.data) {
        Ok(res) => res.into(),
        Err(e) => TokenStream::from(e.to_compile_error()),
    }
}

fn generate_token(name: Ident, data: Data) -> std::result::Result<proc_macro2::TokenStream, Error> {
    let creat_name = Ident::new("configuration", proc_macro2::Span::call_site());
    let fields = get_struct_fields(name.span(), &data)?;
    let update_fn = update(&fields, &creat_name)?;
    let diff_fn = diff(&fields, &creat_name)?;
    Ok(quote! {
        impl #creat_name::Configuration for #name {
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
            "expect derive Configuration on struct with named fields!",
        ))
    }
}

fn update(
    fields: &Punctuated<Field, Comma>,
    creat_name: &Ident,
) -> Result<proc_macro2::TokenStream> {
    let incoming = Ident::new("incoming", proc_macro2::Span::call_site());
    let mut update_fields = Vec::with_capacity(fields.len());
    for field in fields {
        let (skip, submodule) = get_attrs(&field.attrs)?;
        if skip || field.ident.is_none() {
            continue;
        }
        let name = field.ident.as_ref().unwrap();
        let name_lit = LitStr::new(&format!("{}", name), name.span());
        let f = if submodule {
            quote! {
                if let Some(#creat_name::ConfigValue::Module(v)) = #incoming.remove(#name_lit) {
                    #creat_name::Configuration::update(&mut self.#name, v);
                }
            }
        } else {
            quote! {
                if let Some(v) = #incoming.remove(#name_lit) {
                    self.#name = v.into();
                }
            }
        };
        update_fields.push(f);
    }
    Ok(quote! {
        fn update(&mut self, mut #incoming: #creat_name::ConfigChange) {
            #(#update_fields)*
        }
    })
}

fn diff(fields: &Punctuated<Field, Comma>, creat_name: &Ident) -> Result<proc_macro2::TokenStream> {
    let diff_ident = Ident::new("diff_ident", proc_macro2::Span::call_site());
    let incoming = Ident::new("incoming", proc_macro2::Span::call_site());
    let mut diff_fields = Vec::with_capacity(fields.len());
    for field in fields {
        let (skip, submodule) = get_attrs(&field.attrs)?;
        if skip || field.ident.is_none() {
            continue;
        }
        let name = field.ident.as_ref().unwrap();
        let name_lit = LitStr::new(&format!("{}", name), name.span());
        let f = if submodule {
            quote! {
                {
                    let diff = #creat_name::Configuration::diff(&self.#name, &#incoming.#name);
                    if diff.len() != 0 {
                        #diff_ident.insert(#name_lit.to_owned(), #creat_name::ConfigValue::from(diff));
                    }
                }
            }
        } else {
            quote! {
                if self.#name != #incoming.#name {
                    #diff_ident.insert(#name_lit.to_owned(), #creat_name::ConfigValue::from(#incoming.#name.clone()));
                }
            }
        };
        diff_fields.push(f);
    }
    Ok(quote! {
        fn diff(&self, mut #incoming: &Self) -> #creat_name::ConfigChange {
            let mut #diff_ident = std::collections::HashMap::default();
            #(#diff_fields)*
            #diff_ident
        }
    })
}

fn get_attrs(attrs: &[Attribute]) -> Result<(bool, bool)> {
    let (mut skip, mut submodule) = (false, false);
    for attr in attrs {
        if !is_config_attr(&attr) {
            continue;
        }
        let name = attr.parse_args::<Ident>()?;
        if name == "skip" {
            skip = true;
        } else if name == "submodule" {
            submodule = true;
        } else {
            return Err(Error::new(
                name.span(),
                "expect #[config(skip)] or #[config(submodule)]",
            ));
        }
    }
    Ok((skip, submodule))
}

fn is_config_attr(attr: &Attribute) -> bool {
    for s in &attr.path.segments {
        if s.ident == "config" {
            return true;
        }
    }
    false
}

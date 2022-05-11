// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate provides a macro that can be used to generate code to
//! implement `OnlineConfig` trait

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{punctuated::Punctuated, token::Comma, *};

#[proc_macro_derive(OnlineConfig, attributes(online_config))]
pub fn config(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    match generate_token(parse_macro_input!(input as DeriveInput)) {
        Ok(res) => res.into(),
        Err(e) => proc_macro::TokenStream::from(e.to_compile_error()),
    }
}

fn generate_token(ast: DeriveInput) -> std::result::Result<TokenStream, Error> {
    let name = &ast.ident;
    check_generics(&ast.generics, name.span())?;

    let crate_name = Ident::new("online_config", Span::call_site());
    let encoder_name = Ident::new(
        {
            // Avoid naming conflict
            let mut hasher = DefaultHasher::new();
            format!("{}", &name).hash(&mut hasher);
            format!("{}_encoder_{:x}", name, hasher.finish()).as_str()
        },
        Span::call_site(),
    );
    let encoder_lt = Lifetime::new("'lt", Span::call_site());

    let fields = get_struct_fields(ast.data, name.span())?;
    let update_fn = update(&fields, &crate_name)?;
    let diff_fn = diff(&fields, &crate_name)?;
    let get_encoder_fn = get_encoder(&encoder_name, &encoder_lt);
    let typed_fn = typed(&fields, &crate_name)?;
    let encoder_struct = encoder(
        name,
        &crate_name,
        &encoder_name,
        &encoder_lt,
        ast.attrs,
        fields,
    )?;

    Ok(quote! {
        impl<#encoder_lt> #crate_name::OnlineConfig<#encoder_lt> for #name {
            type Encoder = #encoder_name<#encoder_lt>;
            #update_fn
            #diff_fn
            #get_encoder_fn
            #typed_fn
        }
        #encoder_struct
    })
}

fn check_generics(g: &Generics, sp: Span) -> Result<()> {
    if !g.params.is_empty() || g.where_clause.is_some() {
        return Err(Error::new(
            sp,
            "can not derive OnlineConfig on struct with generics type",
        ));
    }
    Ok(())
}

fn get_struct_fields(
    data: Data,
    span: Span,
) -> std::result::Result<Punctuated<Field, Comma>, Error> {
    if let Data::Struct(DataStruct {
        fields: Fields::Named(FieldsNamed { named, .. }),
        ..
    }) = data
    {
        Ok(named)
    } else {
        Err(Error::new(
            span,
            "expect derive OnlineConfig on struct with named fields!",
        ))
    }
}

fn encoder(
    name: &Ident,
    crate_name: &Ident,
    encoder_name: &Ident,
    lt: &Lifetime,
    attrs: Vec<Attribute>,
    fields: Punctuated<Field, Comma>,
) -> Result<TokenStream> {
    let from_ident = Ident::new("source", Span::call_site());
    let mut construct_fields = Vec::with_capacity(fields.len());
    let mut serialize_fields = Vec::with_capacity(fields.len());
    for mut field in fields {
        let (_, hidden, submodule) = get_config_attrs(&field.attrs)?;
        if hidden || field.ident.is_none() {
            continue;
        }
        let field_name = field.ident.as_ref().unwrap();

        construct_fields.push(if submodule {
            quote! { #field_name: #from_ident.#field_name.get_encoder() }
        } else {
            quote! { #field_name: &#from_ident.#field_name }
        });

        field.ty = {
            let ty = &field.ty;
            if submodule {
                Type::Verbatim(quote! { <#ty as #crate_name::OnlineConfig<#lt>>::Encoder })
            } else {
                Type::Verbatim(quote! { &#lt #ty })
            }
        };
        // Only reserve attributes that related to `serde`
        field.attrs = field
            .attrs
            .into_iter()
            .filter(|f| is_attr("serde", f))
            .collect();
        serialize_fields.push(field);
    }
    // Only reserve attributes that related to `serde`
    let attrs: Vec<_> = attrs.into_iter().filter(|a| is_attr("serde", a)).collect();

    Ok(quote! {
        #[doc(hidden)]
        #[derive(serde::Serialize)]
        #(#attrs)*
        pub struct #encoder_name<#lt> {
            #(#serialize_fields,)*
        }

        impl<#lt> From<&#lt #name> for #encoder_name<#lt> {
            fn from(#from_ident: &#lt #name) -> #encoder_name<#lt> {
                #encoder_name {
                    #(#construct_fields,)*
                }
            }
        }
    })
}

fn get_encoder(encoder_name: &Ident, lt: &Lifetime) -> TokenStream {
    quote! {
        fn get_encoder(&#lt self) -> Self::Encoder {
            #encoder_name::from(self)
        }
    }
}

fn update(fields: &Punctuated<Field, Comma>, crate_name: &Ident) -> Result<TokenStream> {
    let incoming = Ident::new("incoming", Span::call_site());
    let mut update_fields = Vec::with_capacity(fields.len());
    for field in fields {
        let (skip, hidden, submodule) = get_config_attrs(&field.attrs)?;
        if skip || hidden || field.ident.is_none() {
            continue;
        }
        let name = field.ident.as_ref().unwrap();
        let name_lit = LitStr::new(&format!("{}", name), name.span());
        let f = if submodule {
            quote! {
                if let Some(#crate_name::ConfigValue::Module(v)) = #incoming.remove(#name_lit) {
                    #crate_name::OnlineConfig::update(&mut self.#name, v);
                }
            }
        } else if is_option_type(&field.ty) {
            quote! {
                if let Some(v) = #incoming.remove(#name_lit) {
                    if #crate_name::ConfigValue::None == v {
                        self.#name = None;
                    } else {
                        self.#name = Some(v.into());
                    }
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
        fn update(&mut self, mut #incoming: #crate_name::ConfigChange) {
            #(#update_fields)*
        }
    })
}

fn diff(fields: &Punctuated<Field, Comma>, crate_name: &Ident) -> Result<TokenStream> {
    let diff_ident = Ident::new("diff_ident", Span::call_site());
    let incoming = Ident::new("incoming", Span::call_site());
    let mut diff_fields = Vec::with_capacity(fields.len());
    for field in fields {
        let (skip, hidden, submodule) = get_config_attrs(&field.attrs)?;
        if skip || hidden || field.ident.is_none() {
            continue;
        }
        let name = field.ident.as_ref().unwrap();
        let name_lit = LitStr::new(&format!("{}", name), name.span());
        let f = if submodule {
            quote! {
                {
                    let diff = #crate_name::OnlineConfig::diff(&self.#name, &#incoming.#name);
                    if diff.len() != 0 {
                        #diff_ident.insert(#name_lit.to_owned(), #crate_name::ConfigValue::from(diff));
                    }
                }
            }
        } else if is_option_type(&field.ty) {
            quote! {
                if self.#name != #incoming.#name {
                    if let Some(ref v) = #incoming.#name {
                        #diff_ident.insert(#name_lit.to_owned(), #crate_name::ConfigValue::from(v.clone()));
                    } else {
                        #diff_ident.insert(#name_lit.to_owned(), #crate_name::ConfigValue::None);
                    }
                }
            }
        } else {
            quote! {
                if self.#name != #incoming.#name {
                    #diff_ident.insert(#name_lit.to_owned(), #crate_name::ConfigValue::from(#incoming.#name.clone()));
                }
            }
        };
        diff_fields.push(f);
    }
    Ok(quote! {
        #[allow(clippy::float_cmp)]
        fn diff(&self, mut #incoming: &Self) -> #crate_name::ConfigChange {
            let mut #diff_ident = std::collections::HashMap::default();
            #(#diff_fields)*
            #diff_ident
        }
    })
}

fn typed(fields: &Punctuated<Field, Comma>, crate_name: &Ident) -> Result<TokenStream> {
    let typed_ident = Ident::new("typed_ident", Span::call_site());
    let mut typed_fields = Vec::with_capacity(fields.len());
    for field in fields {
        let (skip, hidden, submodule) = get_config_attrs(&field.attrs)?;
        if field.ident.is_none() {
            continue;
        }
        let name = field.ident.as_ref().unwrap();
        let name_lit = LitStr::new(&format!("{}", name), name.span());
        let f = if submodule {
            quote! {
                {
                    let typed = #crate_name::OnlineConfig::typed(&self.#name);
                    #typed_ident.insert(#name_lit.to_owned(), #crate_name::ConfigValue::from(typed));
                }
            }
        } else if skip || hidden {
            quote! {
                #typed_ident.insert(#name_lit.to_owned(), #crate_name::ConfigValue::Skip);
            }
        } else if is_option_type(&field.ty) {
            quote! {
                #typed_ident.insert(#name_lit.to_owned(), #crate_name::ConfigValue::from(self.#name.clone().unwrap_or_default()));
            }
        } else {
            quote! {
                #typed_ident.insert(#name_lit.to_owned(), #crate_name::ConfigValue::from(self.#name.clone()));
            }
        };
        typed_fields.push(f);
    }
    Ok(quote! {
        fn typed(&self) -> #crate_name::ConfigChange {
            let mut #typed_ident = std::collections::HashMap::default();
            #(#typed_fields)*
            #typed_ident
        }
    })
}

fn get_config_attrs(attrs: &[Attribute]) -> Result<(bool, bool, bool)> {
    let (mut skip, mut hidden, mut submodule) = (false, false, false);
    for attr in attrs {
        if !is_attr("online_config", attr) {
            continue;
        }
        match attr.parse_args::<Ident>()? {
            name if name == "skip" => skip = true,
            name if name == "hidden" => hidden = true,
            name if name == "submodule" => submodule = true,
            name => {
                return Err(Error::new(
                    name.span(),
                    "expect #[online_config(skip)], #[online_config(hidden)] or #[online_config(submodule)]",
                ));
            }
        }
    }
    Ok((skip, hidden, submodule))
}

fn is_attr(name: &str, attr: &Attribute) -> bool {
    for s in &attr.path.segments {
        if s.ident == name {
            return true;
        }
    }
    false
}

// Copied from https://stackoverflow.com/questions/55271857/how-can-i-get-the-t-from-an-optiont-when-using-syn.
fn is_option_type(ty: &Type) -> bool {
    fn extract_type_path(ty: &syn::Type) -> Option<&Path> {
        match *ty {
            syn::Type::Path(ref typepath) if typepath.qself.is_none() => Some(&typepath.path),
            _ => None,
        }
    }

    // TODO store (with lazy static) the vec of string
    // TODO maybe optimization, reverse the order of segments
    fn extract_option_segment(path: &Path) -> Option<&PathSegment> {
        let idents_of_path = path
            .segments
            .iter()
            .into_iter()
            .fold(String::new(), |mut acc, v| {
                acc.push_str(&v.ident.to_string());
                acc.push('|');
                acc
            });
        vec!["Option|", "std|option|Option|", "core|option|Option|"]
            .into_iter()
            .find(|s| idents_of_path == *s)
            .and_then(|_| path.segments.last())
    }

    extract_type_path(ty)
        .and_then(|path| extract_option_segment(path))
        .is_some()
}

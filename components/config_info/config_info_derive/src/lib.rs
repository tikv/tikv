// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate provides a macro that can be used to generate code to
//! implement `ConfigInfo` trait

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn::{punctuated::Punctuated, spanned::Spanned, token::Comma, *};

const CRATE_NAME: &'static str = "config_info";

#[proc_macro_derive(ConfigInfo, attributes(config_info))]
pub fn config(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    match generate_token(parse_macro_input!(input as DeriveInput)) {
        Ok(res) => res.into(),
        Err(e) => proc_macro::TokenStream::from(e.to_compile_error()),
    }
}

fn generate_token(ast: DeriveInput) -> std::result::Result<TokenStream, Error> {
    let name = &ast.ident;
    check_generics(&ast.generics, name.span())?;

    let crate_name = Ident::new(CRATE_NAME, Span::call_site());
    let encoder_name = Ident::new(
        {
            // Avoid naming conflict
            let mut hasher = DefaultHasher::new();
            format!("{}", &name).hash(&mut hasher);
            format!("{}_cfg_encoder_{:x}", name, hasher.finish()).as_str()
        },
        Span::call_site(),
    );

    let fields = get_struct_fields(ast.data, name.span())?;
    let get_encoder_fn = get_encoder(&encoder_name);
    let encoder_struct = encoder(name, &crate_name, &encoder_name, ast.attrs, fields)?;

    Ok(quote! {
        impl #crate_name::ConfigInfo for #name {
            type Encoder = #encoder_name;
            #get_encoder_fn
        }
        #encoder_struct
    })
}

fn check_generics(g: &Generics, sp: Span) -> Result<()> {
    if !g.params.is_empty() || g.where_clause.is_some() {
        return Err(Error::new(
            sp,
            "can not derive ConfigInfo on struct with generics type",
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
            "expect derive ConfigInfo on struct with named fields!",
        ))
    }
}

fn encoder(
    name: &Ident,
    crate_name: &Ident,
    encoder_name: &Ident,
    mut attrs: Vec<Attribute>,
    fields: Punctuated<Field, Comma>,
) -> Result<TokenStream> {
    let from_ident = Ident::new("source", Span::call_site());
    let target_ident = Ident::new("target", Span::call_site());
    let mut construct_fields = Vec::with_capacity(fields.len());
    let mut serialize_fields = Vec::with_capacity(fields.len());
    for mut field in fields {
        let cfg_attrs = get_config_attrs(&field.attrs)?;
        if cfg_attrs.skipped {
            continue;
        }
        let field_name = field.ident.as_ref().unwrap();
        let (optional, real_type) = {
            let inner_type = extract_option_inner_type(&field.ty);
            (inner_type.is_some(), inner_type.unwrap_or(field.ty.clone()))
        };

        construct_fields.push(if cfg_attrs.is_sub_module {
            quote! { #field_name: #from_ident.#field_name.get_cfg_encoder(&#target_ident.#field_name) }
        } else {
            let description = fetch_doc_comment(field.span(), &field.attrs)?;
            let cons = build_filed_constructer(
                crate_name,
                &from_ident,
                &target_ident,
                field_name,
                &real_type,
                optional,
                &cfg_attrs,
                description,
            )?;
            quote! { #field_name: #cons }
        });

        field.ty = {
            if cfg_attrs.is_sub_module {
                Type::Verbatim(quote! { <#real_type as #crate_name::ConfigInfo>::Encoder })
            } else {
                Type::Verbatim(quote! { config_info::FieldInfo<#real_type> })
            }
        };
        // Only reserve attributes that related to `serde`
        field.attrs.retain(|a| is_attr("serde", a));
        serialize_fields.push(field);
    }
    // Only reserve attributes that related to `serde`
    attrs.retain(|a| is_attr("serde", a));

    Ok(quote! {
        #[doc(hidden)]
        #[derive(serde::Serialize)]
        #(#attrs)*
        pub struct #encoder_name {
            #(#serialize_fields,)*
        }

        impl #encoder_name {
            fn new(#from_ident: &#name, #target_ident: &#name) -> #encoder_name {
                #encoder_name {
                    #(#construct_fields,)*
                }
            }
        }
    })
}

fn build_filed_constructer(
    crate_name: &Ident,
    source: &Ident,
    target: &Ident,
    field_name: &Ident,
    inner_type: &Type,
    optional: bool,
    attrs: &CfgAttrs,
    desc: String,
) -> Result<TokenStream> {
    let CfgAttrs {
        is_sub_module: _,
        skipped: _,
        min_value,
        max_value,
        value_options,
        field_type,
    } = attrs;
    let convert_field = |l: &Lit| match l {
        Lit::Str(s) => {
            let value = s.value();
            quote!(core::convert::TryInto::try_into(#value).unwrap())
        }
        _ => l.to_token_stream(),
    };
    let default_value = if !optional {
        quote!(Some(#source.#field_name.clone()))
    } else {
        quote!(#source.#field_name.clone())
    };
    let value_in_file = quote!( if #source.#field_name != #target.#field_name{ #target.#field_name.clone().into() } else {None});
    let min_value = min_value
        .as_ref()
        .map(|l| {
            let value_tokens = convert_field(l);
            quote! { .set_min_value(#value_tokens) }
        })
        .unwrap_or_default();
    let max_value = max_value
        .as_ref()
        .map(|l| {
            let value_tokens = convert_field(l);
            quote! { .set_max_value(#value_tokens) }
        })
        .unwrap_or_default();
    let options_token = value_options
        .as_deref()
        .map(|ls: &[Lit]| {
            let value_tokens: Vec<_> = ls.iter().map(convert_field).collect();
            quote! { .set_value_options([ #(#value_tokens,)* ].into()) }
        })
        .unwrap_or_default();
    let ft_token = if let Some(ft) = field_type {
        quote!(#ft)
    } else {
        convert_to_config_type(inner_type)?
    };
    Ok(
        quote! { #crate_name::FieldInfo::new(config_info::FieldCfgType::#ft_token, #default_value, #value_in_file, #desc.into())#min_value#max_value#options_token },
    )
}

fn get_encoder(encoder_name: &Ident) -> TokenStream {
    quote! {
        fn get_cfg_encoder(&self, other: &Self) -> Self::Encoder {
            #encoder_name::new(self, other)
        }
    }
}

#[derive(Default)]
struct CfgAttrs {
    is_sub_module: bool,
    skipped: bool,
    min_value: Option<Lit>,
    max_value: Option<Lit>,
    value_options: Option<Vec<Lit>>,
    field_type: Option<Path>,
}

fn get_config_attrs(attrs: &[Attribute]) -> Result<CfgAttrs> {
    let mut cfg_attrs = CfgAttrs::default();
    for attr in attrs {
        if !is_attr(CRATE_NAME, attr) {
            continue;
        }
        match attr.parse_meta()? {
            // #[config_info]
            Meta::Path(p) => {
                assert_eq!(p.get_ident().unwrap(), CRATE_NAME);
            }
            // #[config_info(min=..., max=..., options=...)]
            Meta::List(l) => {
                for inner in l.nested {
                    match inner {
                        NestedMeta::Meta(Meta::Path(p)) if p.is_ident("submodule") => {
                            cfg_attrs.is_sub_module = true;
                        }
                        NestedMeta::Meta(Meta::Path(p)) if p.is_ident("skip") => {
                            cfg_attrs.skipped = true;
                        }
                        NestedMeta::Meta(Meta::NameValue(nv)) => {
                            match &*format!("{}", nv.path.get_ident().unwrap()) {
                                "min" => {
                                    cfg_attrs.min_value = Some(nv.lit);
                                }
                                "max" => {
                                    cfg_attrs.max_value = Some(nv.lit);
                                }
                                "options" => {
                                    let span = nv.lit.span();
                                    let options = parse_value_options(nv.lit)?;
                                    if options.is_empty() {
                                        return Err(Error::new(span, "options must not be empty"));
                                    }
                                    cfg_attrs.value_options = Some(options);
                                }
                                "type" => {
                                    cfg_attrs.field_type = Some(parse_cfg_type(&nv.lit)?);
                                }
                                _ => {
                                    return Err(Error::new(
                                        nv.span(),
                                        format!(
                                            "unknown attribute '{:?}', expect #[config_info(type=.., min=.., max=.., options=..)]",
                                            &nv.path
                                        ),
                                    ));
                                }
                            }
                        }
                        _ => {
                            return Err(Error::new(
                                inner.span(),
                                "expect #[config_info(submodule)] or #[config_info(default=.., min=.., max=..)]",
                            ));
                        }
                    }
                }
            }
            Meta::NameValue(_) => unreachable!(),
        }
    }
    Ok(cfg_attrs)
}

fn parse_cfg_type(value: &Lit) -> Result<Path> {
    let error = || -> Result<Path> {
        Err(Error::new(
            value.span(),
            format!(
                "unknown config type '{:?}', expect one of [Array, Boolean, Map, Number, String]",
                value
            ),
        ))
    };
    match value {
        Lit::Str(s) => {
            let value = s.value();
            match &*value {
                "Array" | "Boolean" | "Map" | "Number" | "String" => parse_str(&*value),
                _ => error(),
            }
        }
        _ => error(),
    }
}

fn fetch_doc_comment(field_span: Span, attrs: &[Attribute]) -> Result<String> {
    for attr in attrs {
        if !is_attr("doc", attr) {
            continue;
        }
        if let Meta::NameValue(nv) = attr.parse_meta()? {
            if let Lit::Str(s) = nv.lit {
                // TODO: maybe we need to return all the docs instead of only the first line.
                return Ok(s.value());
            } else {
                return Err(Error::new(
                    nv.span(),
                    "invalid doc attributes. Please add docs for this field like '/// ...' or #[doc = \"...\"]",
                ));
            }
        }
    }
    Err(Error::new(
        field_span,
        "doc attribute not found. Please add docs for this field like '/// ...' or #[doc = \"...\"]",
    ))
}

fn parse_value_options(tokens: Lit) -> Result<Vec<Lit>> {
    if let Lit::Str(s) = tokens {
        let options = parse_str::<ValueOptions>(&s.value())?;
        Ok(options.options.into_iter().collect())
    } else {
        Err(Error::new(
            tokens.span(),
            "expected literal string with format [option1, option2, ...]",
        ))
    }
}

struct ValueOptions {
    _bracket_token: token::Bracket,
    options: Punctuated<Lit, Token![,]>,
}

impl parse::Parse for ValueOptions {
    fn parse(input: parse::ParseStream) -> Result<Self> {
        let content;
        Ok(Self {
            _bracket_token: bracketed!(content in input),
            options: Punctuated::parse_terminated(&content)?,
        })
    }
}

fn is_attr(name: &str, attr: &Attribute) -> bool {
    for s in &attr.path.segments {
        if s.ident == name {
            return true;
        }
    }
    false
}

// extract the inner generic type Inner from Outer<Inner>
// inspired by https://stackoverflow.com/questions/55271857/how-can-i-get-the-t-from-an-optiont-when-using-syn.
fn extract_inner_type(ty: &Type, type_paths: &[&str]) -> Option<Type> {
    fn extract_type_path(ty: &syn::Type) -> Option<&Path> {
        match *ty {
            Type::Path(ref typepath) if typepath.qself.is_none() => Some(&typepath.path),
            _ => None,
        }
    }

    // TODO store (with lazy static) the vec of string
    // TODO maybe optimization, reverse the order of segments
    fn extract_option_segment<'a>(path: &'a Path, type_paths: &[&str]) -> Option<&'a PathSegment> {
        let idents_of_path = path
            .segments
            .iter()
            .into_iter()
            .fold(String::new(), |mut acc, v| {
                acc.push_str(&v.ident.to_string());
                acc.push('|');
                acc
            });

        type_paths
            .into_iter()
            .find(|s| idents_of_path == **s)
            .and_then(|_| path.segments.last())
    }

    extract_type_path(ty)
        .and_then(|path| extract_option_segment(path, type_paths))
        .map(|seg| {
            let generic_arg = match &seg.arguments {
                PathArguments::AngleBracketed(params) => params.args.iter().next().unwrap().clone(),
                _ => unreachable!(),
            };
            match generic_arg {
                GenericArgument::Type(ty) => ty,
                _ => unreachable!(),
            }
        })
}

fn extract_option_inner_type(ty: &Type) -> Option<Type> {
    const OPTION_TYPES: [&'static str; 3] =
        ["Option|", "std|option|Option|", "core|option|Option|"];
    extract_inner_type(ty, &OPTION_TYPES)
}

fn convert_to_config_type(ty: &Type) -> Result<TokenStream> {
    let real_type = extract_option_inner_type(ty).unwrap_or(ty.clone());

    let match_primitive_ident = |ident: &Ident| match &*format!("{}", ident) {
        "i8" | "u8" | "i16" | "u16" | "i32" | "u32" | "i64" | "u64" | "i128" | "u128" | "isize"
        | "usize" => Ok(quote!(Number)),
        "bool" => Ok(quote!(Boolean)),
        "String" | "ReadableSize" | "ReadableDuration" => Ok(quote!(String)),
        _ => Err(Error::new(
            ident.span(),
            format!(
                "unknown type '{}', please manually add #[config_info(type = \"\")] for this field",
                ident
            ),
        )),
    };

    match real_type {
        Type::Array(_) => Ok(quote!(Array)),
        Type::Path(p) if p.qself.is_none() => {
            if let Some(ident) = p.path.get_ident() {
                match_primitive_ident(ident)
            } else {
                // check the last segment
                let segment = p.path.segments.iter().last().unwrap();
                if let PathArguments::None = segment.arguments {
                    match_primitive_ident(&segment.ident)
                } else {
                    match &*format!("{}", segment.ident) {
                        "Vec" => Ok(quote!(Array)),
                        "HashMap" | "BTreeMap" => Ok(quote!(Map)),
                        _ => Err(Error::new(
                            p.span(),
                            format!(
                                "can not parse config type for '{}', please manually add #[config_info(type = \"..\")] for this field",
                                segment.ident
                            ),
                        )),
                    }
                }
            }
        }
        _ => unreachable!(),
    }
}

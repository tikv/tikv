// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate provides a macro that can be used to generate code to
//! implement `ConfigInfo` trait

#![feature(vec_retain_mut)]

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn::{punctuated::Punctuated, spanned::Spanned, token::Comma, *};

const CRATE_NAME: &str = "config_info";

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
    let default_ident = Ident::new("default_value", Span::call_site());
    let mut construct_fields = Vec::with_capacity(fields.len());
    let mut serialize_fields = Vec::with_capacity(fields.len());
    let mut new_types = Vec::new();
    for mut field in fields {
        let cfg_attrs = get_config_attrs(&field.attrs)?;
        if cfg_attrs.skipped {
            continue;
        }
        let (field_constructer, new_type_tokens) = build_encoder_field(
            crate_name,
            &from_ident,
            &default_ident,
            &mut field,
            cfg_attrs,
        )?;

        construct_fields.push(field_constructer);
        serialize_fields.push(field);
        new_types.push(new_type_tokens);
    }
    // Only reserve attributes that related to `serde`
    attrs.retain(|a| is_attr("serde", a));

    Ok(quote! {
        #(#new_types)*
        #[doc(hidden)]
        #[derive(serde::Serialize)]
        #(#attrs)*
        pub struct #encoder_name {
            #(#serialize_fields,)*
        }

        impl #encoder_name {
            fn new(#from_ident: &#name) -> #encoder_name {
                let #default_ident = #name::default();
                #encoder_name {
                    #(#construct_fields,)*
                }
            }
        }
    })
}

// (TokenStream, TokenStrem) stands for (field constructor statement, new related type definition(if any))
fn build_encoder_field(
    crate_name: &Ident,
    source: &Ident,
    default: &Ident,
    field: &mut Field,
    cfg_attrs: CfgAttrs,
) -> Result<(TokenStream, TokenStream)> {
    let field_name = field.ident.as_ref().unwrap();
    let (optional, real_type) = {
        let inner_type = extract_option_inner_type(&field.ty);
        (
            inner_type.is_some(),
            inner_type.unwrap_or_else(|| field.ty.clone()),
        )
    };

    if cfg_attrs.is_sub_module {
        field.ty = Type::Verbatim(quote! { <#real_type as #crate_name::ConfigInfo>::Encoder });
        rewrite_field_attr(&mut field.attrs);
        return Ok((
            quote! { #field_name: #source.#field_name.get_cfg_encoder() },
            quote! {},
        ));
    }

    let CfgAttrs {
        default_value_desc,
        min_value,
        min_value_desc,
        max_value,
        max_value_desc,
        value_options,
        field_type,
        serialize,
        deserialize,
        ..
    } = cfg_attrs;

    field.ty = Type::Verbatim(quote! { config_info::FieldInfo<#real_type> });

    let description = fetch_doc_comment(field_name.span(), &field.attrs)?;

    let mut wrap_field_name = None;
    let (enc_field_type, new_type_tokens) = if let Some(serialize) = &serialize {
        let new_field_name = Ident::new(
            &format!("_{}_info_{:016x}", &field_name, rand::random::<usize>()),
            field_name.span(),
        );
        wrap_field_name = Some(new_field_name.clone());

        (
            Type::Verbatim(quote! { config_info::FieldInfo<#new_field_name> }),
            quote! {
                 #[doc(hidden)]
                 #[allow(non_camel_case_types)]
                struct #new_field_name(#real_type);
                impl From<#real_type> for #new_field_name {
                    fn from(v: #real_type) -> Self {
                        Self(v)
                    }
                }
                impl serde::Serialize for #new_field_name {
                    fn serialize<S>(&self, _s: S) -> std::result::Result<S::Ok, S::Error> where S: serde::ser::Serializer {
                        #serialize(&self.0, _s)
                    }
                }
            },
        )
    } else {
        (
            Type::Verbatim(quote! { config_info::FieldInfo<#real_type> }),
            quote!(),
        )
    };

    let convert_field = |l: Lit| {
        let converted = match l {
            Lit::Int(mut i) => {
                if i.suffix().is_empty() {
                    // always treat untyped int value as i64
                    i = LitInt::new(&format!("{}i64", i.base10_digits()), i.span())
                };
                Lit::Int(i)
            }
            Lit::Float(mut f) => {
                if f.suffix().is_empty() {
                    // always treat untyped float value as f64
                    f = LitFloat::new(&format!("{}f64", f.base10_digits()), f.span());
                };
                Lit::Float(f)
            }
            l => l,
        };
        let value_token = quote!(serde::de::IntoDeserializer::<serde::de::value::Error>::into_deserializer(#converted));
        let tokens = if let Some(deserialize) = &deserialize {
            quote!(#deserialize(#value_token).unwrap())
        } else {
            quote!(<#real_type as serde::Deserialize>::deserialize(#value_token).unwrap())
        };
        if let Some(ref w) = wrap_field_name {
            quote!(#w(#tokens))
        } else {
            tokens
        }
    };
    let default_value = if let Some(value) = default_value_desc {
        quote!(Some(#crate_name::ConfigValue::Desc(#value.into())))
    } else if !optional {
        quote!(Some(#crate_name::ConfigValue::Concrete(#default.#field_name.clone().into())))
    } else {
        quote!(#default.#field_name.clone().map(|v| #crate_name::ConfigValue::Concrete(v.into())))
    };
    let value_in_file = {
        let value_tokens = if let Some(ref wrap_fn) = wrap_field_name {
            if optional {
                quote!(#source.#field_name.clone().map(#wrap_fn))
            } else {
                quote!(Some(#source.#field_name.clone().into()))
            }
        } else {
            quote!(#source.#field_name.clone().into())
        };
        quote!( if #source.#field_name != #default.#field_name{ #value_tokens } else {None})
    };
    let min_value = min_value
        .map(|l| {
            let value_tokens = convert_field(l);
            quote! { .set_min_value(#crate_name::ConfigValue::Concrete(#value_tokens)) }
        })
        .or_else(|| {
            min_value_desc
                .map(|l| quote! { .set_min_value(#crate_name::ConfigValue::Desc(#l.into())) })
        })
        .unwrap_or_default();
    let max_value = max_value
        .map(|l| {
            let value_tokens = convert_field(l);
            quote! { .set_max_value(#crate_name::ConfigValue::Concrete(#value_tokens)) }
        })
        .or_else(|| {
            max_value_desc
                .map(|l| quote! { .set_max_value(#crate_name::ConfigValue::Desc(#l.into())) })
        })
        .unwrap_or_default();
    let options_token = value_options
        .map(|ls: Vec<Lit>| {
            let value_tokens: Vec<_> = ls.into_iter().map(convert_field).collect();
            quote! { .set_value_options([ #(#value_tokens,)* ].into()) }
        })
        .unwrap_or_default();
    let ft_token = if let Some(ft) = field_type {
        quote!(#ft)
    } else {
        convert_to_config_type(&real_type)?
    };
    field.ty = enc_field_type;
    field.vis = Visibility::Inherited;
    // only retain attrs that related to serde.
    rewrite_field_attr(&mut field.attrs);
    Ok((
        quote! {#field_name:  #crate_name::FieldInfo::new(#crate_name::FieldCfgType::#ft_token, #default_value, #value_in_file, #description.into())#min_value#max_value#options_token },
        new_type_tokens,
    ))
}

fn get_encoder(encoder_name: &Ident) -> TokenStream {
    quote! {
        fn get_cfg_encoder(&self) -> Self::Encoder {
            #encoder_name::new(self)
        }
    }
}

#[derive(Default)]
struct CfgAttrs {
    is_sub_module: bool,
    skipped: bool,
    default_value_desc: Option<String>,
    min_value: Option<Lit>,
    min_value_desc: Option<String>,
    max_value: Option<Lit>,
    max_value_desc: Option<String>,
    value_options: Option<Vec<Lit>>,
    field_type: Option<Path>,
    deserialize: Option<Path>,
    serialize: Option<Path>,
}

fn get_config_attrs(attrs: &[Attribute]) -> Result<CfgAttrs> {
    let mut cfg_attrs = CfgAttrs::default();
    for attr in attrs {
        if is_attr(CRATE_NAME, attr) {
            parse_config_cfg_attr(attr, &mut cfg_attrs)?;
        } else if is_attr("serde", attr) {
            // extract custom deserializer from serde attributes
            parse_deserialize_attr(attr, &mut cfg_attrs)?;
        }
    }
    Ok(cfg_attrs)
}

fn parse_config_cfg_attr(attr: &Attribute, cfg_attrs: &mut CfgAttrs) -> Result<()> {
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
                            "default_desc" => {
                                cfg_attrs.default_value_desc = Some(get_lit_str(&nv.lit)?);
                            }
                            "min" => {
                                if cfg_attrs.min_value_desc.is_some() {
                                    return Err(Error::new(
                                        nv.span(),
                                        "`min` and `min_desc` should not be both set",
                                    ));
                                }
                                cfg_attrs.min_value = Some(nv.lit);
                            }
                            "min_desc" => {
                                if cfg_attrs.min_value.is_some() {
                                    return Err(Error::new(
                                        nv.span(),
                                        "`min` and `min_desc` should not be both set",
                                    ));
                                }
                                cfg_attrs.min_value_desc = Some(get_lit_str(&nv.lit)?)
                            }
                            "max" => {
                                if cfg_attrs.max_value_desc.is_some() {
                                    return Err(Error::new(
                                        nv.span(),
                                        "`max` and `max_desc` should not be both set",
                                    ));
                                }
                                cfg_attrs.max_value = Some(nv.lit);
                            }
                            "max_desc" => {
                                if cfg_attrs.max_value.is_some() {
                                    return Err(Error::new(
                                        nv.span(),
                                        "`max` and `max_desc` should not be both set",
                                    ));
                                }
                                cfg_attrs.max_value_desc = Some(get_lit_str(&nv.lit)?)
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
                                    format!("unknown attribute '{:?}'", &nv.path),
                                ));
                            }
                        }
                    }
                    _ => {
                        return Err(Error::new(
                            inner.span(),
                            "expect #[config_info(submodule)] or #[config_info(min=.., max=.., ..)]",
                        ));
                    }
                }
            }
        }
        Meta::NameValue(_) => unreachable!(),
    }
    Ok(())
}

// only keep serialize related attributes
fn rewrite_field_attr(attrs: &mut Vec<Attribute>) {
    struct AttrWrapper {
        attrs: Vec<Attribute>,
    }
    impl syn::parse::Parse for AttrWrapper {
        fn parse(input: syn::parse::ParseStream<'_>) -> Result<Self> {
            Ok(Self {
                attrs: input.call(Attribute::parse_outer)?,
            })
        }
    }

    attrs.retain_mut(|attr| {
        if !is_attr("serde", attr) {
            return false;
        }
        match attr.parse_meta() {
            Ok(Meta::List(mut ml)) => {
                let mut changed = false;
                let mut new_nested = Punctuated::new();
                for inner in &ml.nested {
                    if let NestedMeta::Meta(Meta::NameValue(nv)) = inner {
                        match &*format!("{}", nv.path.get_ident().unwrap()) {
                            "with" | "deserialize_with" | "serialize_with" => {
                                changed = true;
                            }
                            _ => {
                                new_nested.push(inner.clone());
                            }
                        }
                    } else {
                        new_nested.push(inner.clone());
                    }
                }
                let is_empty = new_nested.is_empty();
                if !is_empty && changed {
                    ml.nested = new_nested;
                    let mut wrapper: AttrWrapper = parse2(ml.into_token_stream()).unwrap();
                    *attr = wrapper.attrs.pop().unwrap();
                }
                !is_empty
            }
            _ => true,
        }
    });
}

fn parse_deserialize_attr(attr: &Attribute, cfg_attrs: &mut CfgAttrs) -> Result<()> {
    // parse serde attributes like #[serde(with = "..", deserialize_with = "..")]
    if let Ok(Meta::List(ml)) = attr.parse_meta() {
        for inner in ml.nested {
            if let NestedMeta::Meta(Meta::NameValue(nv)) = inner {
                match &*format!("{}", nv.path.get_ident().unwrap()) {
                    "with" => {
                        let path: Path = lit_to_path(&nv.lit)?;
                        cfg_attrs.deserialize = Some(parse2(quote!(#path::deserialize))?);
                        cfg_attrs.serialize = Some(parse2(quote!(#path::serialize))?);
                    }
                    "deserialize_with" => {
                        cfg_attrs.deserialize = Some(lit_to_path(&nv.lit)?);
                    }
                    "serialize_with" => {
                        cfg_attrs.serialize = Some(lit_to_path(&nv.lit)?);
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

fn get_lit_str(value: &Lit) -> Result<String> {
    if let Lit::Str(s) = value {
        Ok(s.value())
    } else {
        Err(Error::new(
            value.span(),
            "invalid literal value, literal string is expected",
        ))
    }
}

fn parse_cfg_type(value: &Lit) -> Result<Path> {
    let valid_values = ["Array", "Boolean", "Map", "Number", "String"];

    let path = lit_to_path(value)?;
    if valid_values.iter().any(|s| path.is_ident(s)) {
        Ok(path)
    } else {
        Err(Error::new(
            value.span(),
            format!(
                "unknown config type '{:?}', expect one of [Array, Boolean, Map, Number, String]",
                value
            ),
        ))
    }
}

fn lit_to_path(value: &Lit) -> Result<Path> {
    match value {
        Lit::Str(s) => parse_str(&*s.value()),
        _ => Err(Error::new(
            value.span(),
            format!("expected literal string, got '{:?}'", value),
        )),
    }
}

fn fetch_doc_comment(field_span: Span, attrs: &[Attribute]) -> Result<String> {
    let mut description = String::new();
    for attr in attrs {
        if !is_attr("doc", attr) {
            continue;
        }
        if let Meta::NameValue(nv) = attr.parse_meta()? {
            // return the first
            if let Lit::Str(s) = nv.lit {
                let value = s.value();
                if !value.trim().is_empty() {
                    description += &value;
                } else if !description.is_empty() {
                    break;
                }
            } else {
                return Err(Error::new(
                    nv.span(),
                    "invalid doc attributes. Please add docs for this field like '/// ...' or #[doc = \"...\"]",
                ));
            }
        }
    }
    if !description.is_empty() {
        Ok(description.trim().into())
    } else {
        Err(Error::new(
            field_span,
            "doc attribute not found. Please add docs for this field like '/// ...' or #[doc = \"...\"]",
        ))
    }
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
    fn parse(input: parse::ParseStream<'_>) -> Result<Self> {
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
            .iter()
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
    const OPTION_TYPES: [&str; 3] = ["Option|", "std|option|Option|", "core|option|Option|"];
    extract_inner_type(ty, &OPTION_TYPES)
}

fn convert_to_config_type(ty: &Type) -> Result<TokenStream> {
    let real_type = extract_option_inner_type(ty).unwrap_or_else(|| ty.clone());

    let match_primitive_ident = |ident: &Ident| match &*format!("{}", ident) {
        "i8" | "u8" | "i16" | "u16" | "i32" | "u32" | "i64" | "u64" | "i128" | "u128" | "isize"
        | "usize" | "f32" | "f64" => Ok(quote!(Number)),
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

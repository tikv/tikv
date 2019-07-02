// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use heck::CamelCase;
use proc_macro2::{Span, TokenStream};
use quote::ToTokens;
use syn::punctuated::Punctuated;
use syn::*;

/// Parses an attribute like `#[rpn_fn(vargs, capture = [ctx, output_rows])`.
#[derive(Debug, Default)]
struct RpnFnAttr {
    is_varg: bool,
    captures: Vec<Expr>,
}

impl parse::Parse for RpnFnAttr {
    fn parse(input: parse::ParseStream<'_>) -> Result<Self> {
        let mut is_varg = false;
        let mut captures = Vec::new();

        let config_items = Punctuated::<Expr, Token![,]>::parse_terminated(input).unwrap();
        for item in config_items {
            match item {
                Expr::Assign(ExprAssign { left, right, .. }) => {
                    let left_str = format!("{}", (&left).into_token_stream());
                    match left_str.as_ref() {
                        "capture" => {
                            if let Expr::Array(ExprArray { elems, .. }) = *right {
                                captures = elems.into_iter().collect();
                            } else {
                                return Err(Error::new_spanned(right, "Expect array expression"));
                            }
                        }
                        _ => {
                            return Err(Error::new_spanned(
                                left,
                                format!("Unknown attribute parameter `{}`", left_str),
                            ));
                        }
                    }
                }
                Expr::Path(ExprPath { path, .. }) => {
                    let path_str = format!("{}", (&path).into_token_stream());
                    match path_str.as_ref() {
                        "varg" => {
                            is_varg = true;
                        }
                        _ => {
                            return Err(Error::new_spanned(
                                path,
                                format!("Unknown attribute parameter `{}`", path_str),
                            ));
                        }
                    }
                }
                _ => {
                    return Err(Error::new_spanned(
                        item,
                        "Expect attributes to be `foo=bar` or `foo`",
                    ));
                }
            }
        }
        Ok(Self { is_varg, captures })
    }
}

mod kw {
    syn::custom_keyword!(Option);
}

/// Parses an evaluable type like `Option<T>`.
struct RpnFnEvaluableType {
    eval_type: TypePath,
}

impl parse::Parse for RpnFnEvaluableType {
    fn parse(input: parse::ParseStream<'_>) -> Result<Self> {
        input.parse::<self::kw::Option>()?;
        input.parse::<Token![<]>()?;
        let eval_type = input.parse::<TypePath>()?;
        input.parse::<Token![>]>()?;
        Ok(Self { eval_type })
    }
}

/// Parses a function signature parameter like `val: &Option<T>`.
struct RpnFnSignatureParam {
    _pat: Pat,
    eval_type: TypePath,
}

impl parse::Parse for RpnFnSignatureParam {
    fn parse(input: parse::ParseStream<'_>) -> Result<Self> {
        let pat = input.parse::<Pat>()?;
        input.parse::<Token![:]>()?;
        input.parse::<Token![&]>()?;
        let et = input.parse::<RpnFnEvaluableType>()?;
        Ok(Self {
            _pat: pat,
            eval_type: et.eval_type,
        })
    }
}

/// Parses a function signature parameter like `val: &[&Option<T>]`.
struct VargsRpnFnSignatureParam {
    _pat: Pat,
    eval_type: TypePath,
}

impl parse::Parse for VargsRpnFnSignatureParam {
    fn parse(input: parse::ParseStream<'_>) -> Result<Self> {
        let pat = input.parse::<Pat>()?;
        input.parse::<Token![:]>()?;
        input.parse::<Token![&]>()?;
        let slice_inner;
        bracketed!(slice_inner in input);
        slice_inner.parse::<Token![&]>()?;
        let et = slice_inner.parse::<RpnFnEvaluableType>()?;
        Ok(Self {
            _pat: pat,
            eval_type: et.eval_type,
        })
    }
}

/// Parses a function signature return type like `Result<Option<T>>`.
struct RpnFnSignatureReturnType {
    eval_type: TypePath,
}

impl parse::Parse for RpnFnSignatureReturnType {
    fn parse(input: parse::ParseStream<'_>) -> Result<Self> {
        input.parse::<Token![->]>()?;
        let tp = input.parse::<Type>()?;
        if let Type::Path(TypePath {
            path: Path { segments, .. },
            ..
        }) = &tp
        {
            let result_type = (*segments.last().unwrap().value()).clone();
            if let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) =
                result_type.arguments
            {
                let et = parse2::<RpnFnEvaluableType>(args.into_token_stream())?;
                Ok(Self {
                    eval_type: et.eval_type,
                })
            } else {
                return Err(Error::new_spanned(
                    tp,
                    "expect angle bracketed path arguments",
                ));
            }
        } else {
            return Err(Error::new_spanned(tp, "expect path"));
        }
    }
}

pub struct RpnFn;

impl RpnFn {
    pub fn transform(attr: TokenStream, item_fn: TokenStream) -> Result<TokenStream> {
        let attr = parse2::<RpnFnAttr>(attr)?;
        let item_fn = parse2::<ItemFn>(item_fn)?;

        // FIXME: The macro cannot handle lifetime definitions now
        if let Some(lifetime) = item_fn.decl.generics.lifetimes().next() {
            return Err(Error::new_spanned(
                lifetime,
                "Lifetime definition is not allowed",
            ));
        }

        if attr.is_varg {
            Ok(VargsRpnFn::new(attr, item_fn)?.generate())
        } else {
            Ok(NormalRpnFn::new(attr, item_fn)?.generate())
        }
    }
}

#[derive(Debug)]
struct VargsRpnFn {
    captures: Vec<Expr>,
    item_fn: ItemFn,
    arg_type: TypePath,
    ret_type: TypePath,
}

impl VargsRpnFn {
    pub fn new(attr: RpnFnAttr, item_fn: ItemFn) -> Result<Self> {
        if item_fn.decl.inputs.len() != attr.captures.len() + 1 {
            return Err(Error::new_spanned(
                item_fn.decl.inputs,
                format!("Expect {} parameters", attr.captures.len() + 1),
            ));
        }

        let fn_arg = item_fn.decl.inputs.iter().nth(attr.captures.len()).unwrap();
        let arg_type =
            parse2::<VargsRpnFnSignatureParam>(fn_arg.into_token_stream()).map_err(|_| {
                Error::new_spanned(fn_arg, "Expect parameter type to be like `&[&Option<T>]`")
            })?;

        let ret_type =
            parse2::<RpnFnSignatureReturnType>((&item_fn.decl.output).into_token_stream())
                .map_err(|_| {
                    Error::new_spanned(
                        &item_fn.decl.output,
                        "Expect return type to be like `Result<Option<T>>`",
                    )
                })?;
        Ok(Self {
            captures: attr.captures,
            item_fn,
            arg_type: arg_type.eval_type,
            ret_type: ret_type.eval_type,
        })
    }

    pub fn generate(self) -> TokenStream {
        vec![
            self.generate_constructor(),
            self.item_fn.into_token_stream(),
        ]
        .into_iter()
        .collect()
    }

    fn generate_constructor(&self) -> TokenStream {
        let constructor_ident = Ident::new(
            &format!("{}_fn_meta", &self.item_fn.ident),
            Span::call_site(),
        );
        let (impl_generics, ty_generics, where_clause) =
            self.item_fn.decl.generics.split_for_impl();
        let ty_generics_turbofish = ty_generics.as_turbofish();
        let fn_ident = &self.item_fn.ident;
        let fn_name = self.item_fn.ident.to_string();
        let arg_type = &self.arg_type;
        quote! {
            pub const fn #constructor_ident #impl_generics ()
            -> crate::coprocessor::dag::rpn_expr::RpnFnMeta
            #where_clause
            {
                #[inline]
                fn run #impl_generics (
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                    vargs_buffer: &mut [usize],
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> #where_clause {
                    let args_len = args.len();
                    assert_eq!(vargs_buffer.len(), args_len);
                    let mut result = Vec::with_capacity(output_rows);
                    for row_index in 0..output_rows {
                        for arg_index in 0..args_len {
                            let scalar_arg = args[arg_index].get_logical_scalar_ref(row_index);
                            let arg: &Option<#arg_type> = Evaluable::borrow_scalar_value_ref(&scalar_arg);
                            vargs_buffer[arg_index] = arg as *const _ as usize;
                        }
                        result.push(#fn_ident(unsafe {
                            &*(vargs_buffer as *const _ as *const [&Option<#arg_type>])
                        })?);
                    }
                    Ok(Evaluable::into_vector_value(result))
                }
                crate::coprocessor::dag::rpn_expr::RpnFnMeta {
                    name: #fn_name,
                    fn_ptr: run #ty_generics_turbofish,
                }
            }
        }
    }
}

#[derive(Debug)]
struct NormalRpnFn {
    captures: Vec<Expr>,
    item_fn: ItemFn,
    fn_trait_ident: Ident,
    evaluator_ident: Ident,
    arg_types: Vec<TypePath>,
    ret_type: TypePath,
}

impl NormalRpnFn {
    pub fn new(attr: RpnFnAttr, item_fn: ItemFn) -> Result<Self> {
        let mut arg_types = Vec::new();
        for fn_arg in item_fn.decl.inputs.iter().skip(attr.captures.len()) {
            let arg_type =
                parse2::<RpnFnSignatureParam>(fn_arg.into_token_stream()).map_err(|_| {
                    Error::new_spanned(fn_arg, "Expect parameter type to be like `&Option<T>`")
                })?;
            arg_types.push(arg_type.eval_type);
        }
        let ret_type =
            parse2::<RpnFnSignatureReturnType>((&item_fn.decl.output).into_token_stream())
                .map_err(|_| {
                    Error::new_spanned(
                        &item_fn.decl.output,
                        "Expect return type to be like `Result<Option<T>>`",
                    )
                })?;
        let camel_name = item_fn.ident.to_string().to_camel_case();
        let fn_trait_ident = Ident::new(&format!("{}_Fn", camel_name), Span::call_site());
        let evaluator_ident = Ident::new(&format!("{}_Evaluator", camel_name), Span::call_site());
        Ok(Self {
            captures: attr.captures,
            item_fn,
            fn_trait_ident,
            evaluator_ident,
            arg_types,
            ret_type: ret_type.eval_type,
        })
    }

    pub fn generate(self) -> TokenStream {
        vec![
            self.generate_fn_trait(),
            self.generate_dummy_fn_trait_impl(),
            self.generate_real_fn_trait_impl(),
            self.generate_evaluator(),
            self.generate_constructor(),
            self.item_fn.into_token_stream(),
        ]
        .into_iter()
        .collect()
    }

    fn generate_fn_trait(&self) -> TokenStream {
        let (impl_generics, _, where_clause) = self.item_fn.decl.generics.split_for_impl();
        let fn_trait_ident = &self.fn_trait_ident;
        quote! {
            trait #fn_trait_ident #impl_generics #where_clause {
                fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue>;
            }
        }
    }

    fn generate_dummy_fn_trait_impl(&self) -> TokenStream {
        let mut generics = self.item_fn.decl.generics.clone();
        generics
            .params
            .push(parse_str("D_: crate::coprocessor::dag::rpn_expr::function::ArgDef").unwrap());
        let fn_name = self.item_fn.ident.to_string();
        let fn_trait_ident = &self.fn_trait_ident;
        let tp_ident = Ident::new("D_", Span::call_site());
        let (_, ty_generics, _) = self.item_fn.decl.generics.split_for_impl();
        let (impl_generics, _, where_clause) = generics.split_for_impl();
        quote! {
            impl #impl_generics #fn_trait_ident #ty_generics for #tp_ident #where_clause {
                default fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    panic!("Cannot apply {} on {:?}", #fn_name, self)
                }
            }
        }
    }

    fn generate_real_fn_trait_impl(&self) -> TokenStream {
        let mut generics = self.item_fn.decl.generics.clone();
        generics
            .params
            .push(LifetimeDef::new(Lifetime::new("'arg_", Span::call_site())).into());
        let mut tp = quote! { crate::coprocessor::dag::rpn_expr::function::Null };
        for (arg_index, arg_type) in self.arg_types.iter().enumerate().rev() {
            let arg_name = Ident::new(&format!("Arg{}_", arg_index), Span::call_site());
            let generic_param = quote! {
                #arg_name: crate::coprocessor::dag::rpn_expr::function::RpnFnArg<
                    Type = &'arg_ Option<#arg_type>
                >
            };
            generics.params.push(parse2(generic_param).unwrap());
            tp = quote! { crate::coprocessor::dag::rpn_expr::function::Arg<#arg_name, #tp> };
        }
        let fn_ident = &self.item_fn.ident;
        let fn_trait_ident = &self.fn_trait_ident;
        let (_, ty_generics, _) = self.item_fn.decl.generics.split_for_impl();
        let (impl_generics, _, where_clause) = generics.split_for_impl();
        let captures = &self.captures;
        let extract =
            (0..self.arg_types.len()).map(|i| Ident::new(&format!("arg{}", i), Span::call_site()));
        let call_arg = extract.clone();
        let ty_generics_turbofish = ty_generics.as_turbofish();
        quote! {
            impl #impl_generics #fn_trait_ident #ty_generics for #tp #where_clause {
                default fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    let arg = &self;
                    let mut result = Vec::with_capacity(output_rows);
                    for row in 0..output_rows {
                        #(let (#extract, arg) = arg.extract(row));*;
                        result.push( #fn_ident #ty_generics_turbofish ( #(#captures,)* #(#call_arg),* )?);
                    }
                    Ok(crate::coprocessor::codec::data_type::Evaluable::into_vector_value(result))
                }
            }
        }
    }

    fn generate_evaluator(&self) -> TokenStream {
        let generics = &self.item_fn.decl.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
        let evaluator_ident = &self.evaluator_ident;
        let fn_trait_ident = &self.fn_trait_ident;
        let ty_generics_turbofish = ty_generics.as_turbofish();
        let generic_types = generics.type_params().map(|type_param| &type_param.ident);
        quote! {
            pub struct #evaluator_ident #impl_generics (
                std::marker::PhantomData <(#(#generic_types),*)>
            ) #where_clause ;

            impl #impl_generics crate::coprocessor::dag::rpn_expr::function::Evaluator
                for #evaluator_ident #ty_generics #where_clause {
                #[inline]
                fn eval(
                    self,
                    def: impl crate::coprocessor::dag::rpn_expr::function::ArgDef,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    #fn_trait_ident #ty_generics_turbofish::eval(def, ctx, output_rows, args, extra)
                }
            }
        }
    }

    fn generate_constructor(&self) -> TokenStream {
        let constructor_ident = Ident::new(
            &format!("{}_fn_meta", &self.item_fn.ident),
            Span::call_site(),
        );
        let (impl_generics, ty_generics, where_clause) =
            self.item_fn.decl.generics.split_for_impl();
        let ty_generics_turbofish = ty_generics.as_turbofish();
        let evaluator_ident = &self.evaluator_ident;
        let mut evaluator =
            quote! { #evaluator_ident #ty_generics_turbofish (std::marker::PhantomData) };
        for arg_index in 0..self.arg_types.len() {
            evaluator = quote! { ArgConstructor::new(#arg_index, #evaluator) };
        }
        let fn_name = self.item_fn.ident.to_string();
        quote! {
            pub const fn #constructor_ident #impl_generics ()
            -> crate::coprocessor::dag::rpn_expr::RpnFnMeta
            #where_clause
            {
                #[inline]
                fn run #impl_generics (
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                    vargs_buffer: &mut [usize],
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> #where_clause {
                    use crate::coprocessor::dag::rpn_expr::function::{ArgConstructor, Evaluator, Null};
                    #evaluator.eval(Null, ctx, output_rows, args, extra)
                }
                crate::coprocessor::dag::rpn_expr::RpnFnMeta {
                    name: #fn_name,
                    fn_ptr: run #ty_generics_turbofish,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests_normal {
    use super::*;

    fn no_generic_fn() -> NormalRpnFn {
        let item_fn = parse_str(
            r#"
            #[inline]
            fn foo(arg0: &Option<Int>, arg1: &Option<Real>) -> crate::coprocessor::Result<Option<Decimal>> {
                Ok(None)
            }
        "#,
        )
        .unwrap();
        NormalRpnFn::new(RpnFnAttr::default(), item_fn).unwrap()
    }

    #[test]
    fn test_no_generic_generate_fn_trait() {
        let gen = no_generic_fn();
        let expected: TokenStream = quote! {
            trait Foo_Fn {
                fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> ;
            }
        };
        assert_eq!(expected.to_string(), gen.generate_fn_trait().to_string());
    }

    #[test]
    fn test_no_generic_generate_dummy_fn_trait_impl() {
        let gen = no_generic_fn();
        let expected: TokenStream = quote! {
            impl<D_: crate::coprocessor::dag::rpn_expr::function::ArgDef> Foo_Fn for D_ {
                default fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    panic!("Cannot apply {} on {:?}", "foo", self)
                }
            }
        };
        assert_eq!(
            expected.to_string(),
            gen.generate_dummy_fn_trait_impl().to_string()
        );
    }

    #[test]
    fn test_no_generic_generate_real_fn_trait_impl() {
        let gen = no_generic_fn();
        let expected: TokenStream = quote! {
            impl<
                'arg_,
                Arg1_: crate::coprocessor::dag::rpn_expr::function::RpnFnArg<Type = & 'arg_ Option<Real> > ,
                Arg0_: crate::coprocessor::dag::rpn_expr::function::RpnFnArg<Type = & 'arg_ Option<Int> >
            > Foo_Fn for crate::coprocessor::dag::rpn_expr::function::Arg<
                Arg0_,
                crate::coprocessor::dag::rpn_expr::function::Arg<
                    Arg1_,
                    crate::coprocessor::dag::rpn_expr::function::Null
                >
            > {
                default fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    let arg = &self;
                    let mut result = Vec::with_capacity(output_rows);
                    for row in 0..output_rows {
                        let (arg0, arg) = arg.extract(row);
                        let (arg1, arg) = arg.extract(row);
                        result.push(foo(arg0, arg1)?);
                    }
                    Ok(crate::coprocessor::codec::data_type::Evaluable::into_vector_value(result))
                }
            }
        };
        assert_eq!(
            expected.to_string(),
            gen.generate_real_fn_trait_impl().to_string()
        );
    }

    #[test]
    fn test_no_generic_generate_evaluator() {
        let gen = no_generic_fn();
        let expected: TokenStream = quote! {
            pub struct Foo_Evaluator ( std::marker::PhantomData <()> ) ;

            impl crate::coprocessor::dag::rpn_expr::function::Evaluator for Foo_Evaluator {
                #[inline]
                fn eval(
                    self,
                    def: impl crate::coprocessor::dag::rpn_expr::function::ArgDef,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    Foo_Fn :: eval(def, ctx, output_rows, args, extra)
                }
            }
        };
        assert_eq!(expected.to_string(), gen.generate_evaluator().to_string());
    }

    #[test]
    fn test_no_generic_generate_constructor() {
        let gen = no_generic_fn();
        let expected: TokenStream = quote! {
            pub const fn foo_fn_meta() -> crate::coprocessor::dag::rpn_expr::RpnFnMeta {
                #[inline]
                fn run(
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                    vargs_buffer: &mut [usize],
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    use crate::coprocessor::dag::rpn_expr::function::{ArgConstructor, Evaluator, Null};
                    ArgConstructor::new(
                        1usize,
                        ArgConstructor::new(
                            0usize,
                            Foo_Evaluator(std::marker::PhantomData)
                        )
                    ).eval(Null, ctx, output_rows, args, extra)
                }
                crate::coprocessor::dag::rpn_expr::RpnFnMeta {
                    name: "foo",
                    fn_ptr: run,
                }
            }
        };
        assert_eq!(expected.to_string(), gen.generate_constructor().to_string());
    }

    fn generic_fn() -> NormalRpnFn {
        let item_fn = parse_str(
            r#"
            fn foo<A: M, B>(arg0: &Option<A::X>) -> Result<Option<B>>
            where B: N<M> {
                Ok(None)
            }
        "#,
        )
        .unwrap();
        NormalRpnFn::new(RpnFnAttr::default(), item_fn).unwrap()
    }

    #[test]
    fn test_generic_generate_fn_trait() {
        let gen = generic_fn();
        let expected: TokenStream = quote! {
            trait Foo_Fn<A: M, B>
            where B: N<M> {
                fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> ;
            }
        };
        assert_eq!(expected.to_string(), gen.generate_fn_trait().to_string());
    }

    #[test]
    fn test_generic_generate_dummy_fn_trait_impl() {
        let gen = generic_fn();
        let expected: TokenStream = quote! {
            impl<
                A: M,
                B,
                D_: crate::coprocessor::dag::rpn_expr::function::ArgDef
            > Foo_Fn<A, B> for D_
            where B: N<M> {
                default fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    panic!("Cannot apply {} on {:?}", "foo", self)
                }
            }
        };
        assert_eq!(
            expected.to_string(),
            gen.generate_dummy_fn_trait_impl().to_string()
        );
    }

    #[test]
    fn test_generic_generate_real_fn_trait_impl() {
        let gen = generic_fn();
        let expected: TokenStream = quote! {
            impl<
                'arg_,
                A: M,
                B,
                Arg0_: crate::coprocessor::dag::rpn_expr::function::RpnFnArg<Type = & 'arg_ Option<A::X> >
            > Foo_Fn<A, B> for crate::coprocessor::dag::rpn_expr::function::Arg<
                Arg0_,
                crate::coprocessor::dag::rpn_expr::function::Null
            > where B: N<M> {
                default fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    let arg = &self;
                    let mut result = Vec::with_capacity(output_rows);
                    for row in 0..output_rows {
                        let (arg0, arg) = arg.extract(row);
                        result.push(foo :: <A, B> (arg0)?);
                    }
                    Ok(crate::coprocessor::codec::data_type::Evaluable::into_vector_value(result))
                }
            }
        };
        assert_eq!(
            expected.to_string(),
            gen.generate_real_fn_trait_impl().to_string()
        );
    }

    #[test]
    fn test_generic_generate_evaluator() {
        let gen = generic_fn();
        let expected: TokenStream = quote! {
            pub struct Foo_Evaluator<A: M, B> (std::marker::PhantomData<(A, B)>)
                where B: N<M> ;

            impl<A: M, B> crate::coprocessor::dag::rpn_expr::function::Evaluator
                for Foo_Evaluator<A, B>
                where B: N<M> {
                #[inline]
                fn eval(
                    self,
                    def: impl crate::coprocessor::dag::rpn_expr::function::ArgDef,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    Foo_Fn :: <A, B> :: eval(def, ctx, output_rows, args, extra)
                }
            }
        };
        assert_eq!(expected.to_string(), gen.generate_evaluator().to_string());
    }

    #[test]
    fn test_generic_generate_constructor() {
        let gen = generic_fn();
        let expected: TokenStream = quote! {
            pub const fn foo_fn_meta <A: M, B> () -> crate::coprocessor::dag::rpn_expr::RpnFnMeta
            where B: N<M> {
                #[inline]
                fn run <A: M, B> (
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                    vargs_buffer: &mut [usize],
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue>
                where B: N<M> {
                    use crate::coprocessor::dag::rpn_expr::function::{ArgConstructor, Evaluator, Null};
                    ArgConstructor::new(
                        0usize,
                        Foo_Evaluator :: < A , B > (std::marker::PhantomData)
                    ).eval(Null, ctx, output_rows, args, extra)
                }
                crate::coprocessor::dag::rpn_expr::RpnFnMeta {
                    name: "foo",
                    fn_ptr: run :: <A, B> ,
                }
            }
        };
        assert_eq!(expected.to_string(), gen.generate_constructor().to_string());
    }

    fn no_generic_fn_with_extras() -> NormalRpnFn {
        let item_fn = parse_str(
            r#"
            #[inline]
            fn foo(ctx: &mut EvalContext, arg0: &Option<Int>, arg1: &Option<Real>) -> Result<Option<Decimal>> {
                Ok(None)
            }
        "#,
        )
            .unwrap();
        NormalRpnFn::new(
            RpnFnAttr {
                is_varg: false,
                captures: vec![parse_str("ctx").unwrap()],
            },
            item_fn,
        )
        .unwrap()
    }

    #[test]
    fn test_no_generic_with_extras_generate_real_fn_trait_impl() {
        let gen = no_generic_fn_with_extras();
        let expected: TokenStream = quote! {
            impl<
                'arg_,
                Arg1_: crate::coprocessor::dag::rpn_expr::function::RpnFnArg<Type = & 'arg_ Option<Real> > ,
                Arg0_: crate::coprocessor::dag::rpn_expr::function::RpnFnArg<Type = & 'arg_ Option<Int> >
            > Foo_Fn for crate::coprocessor::dag::rpn_expr::function::Arg<
                Arg0_,
                crate::coprocessor::dag::rpn_expr::function::Arg<
                    Arg1_,
                    crate::coprocessor::dag::rpn_expr::function::Null
                >
            > {
                default fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::coprocessor::dag::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::coprocessor::dag::rpn_expr::RpnFnCallExtra<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    let arg = &self;
                    let mut result = Vec::with_capacity(output_rows);
                    for row in 0..output_rows {
                        let (arg0, arg) = arg.extract(row);
                        let (arg1, arg) = arg.extract(row);
                        result.push(foo(ctx, arg0, arg1)?);
                    }
                    Ok(crate::coprocessor::codec::data_type::Evaluable::into_vector_value(result))
                }
            }
        };
        assert_eq!(
            expected.to_string(),
            gen.generate_real_fn_trait_impl().to_string()
        );
    }
}

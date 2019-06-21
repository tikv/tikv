// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::Result;
use heck::CamelCase;
use proc_macro2::{Span, TokenStream};
use quote::ToTokens;
use syn::spanned::Spanned;
use syn::{
    parse2, parse_str, FnArg, GenericArgument, Ident, ItemFn, Lifetime, LifetimeDef, PathArguments,
    Type, TypePath,
};

pub struct RpnFnGenerator {
    meta: Vec<Ident>,
    item_fn: ItemFn,
    fn_trait_ident: Ident,
    evaluator_ident: Ident,
    arg_types: Vec<TypePath>,
}

impl RpnFnGenerator {
    pub fn new(meta: Vec<Ident>, item_fn: ItemFn) -> Result<Self> {
        // FIXME: The macro cannot handle lifetime definitions now
        if let Some(lifetime) = item_fn.decl.generics.lifetimes().next() {
            return Err(lifetime
                .span()
                .unwrap()
                .error("Lifetime definition is not allowed"));
        }

        let arg_types = item_fn
            .decl
            .inputs
            .iter()
            .skip(meta.len()) // ctx or payload are not function args
            .map(parse_arg_type)
            .collect::<Result<Vec<_>>>()?;
        let camel_name = item_fn.ident.to_string().to_camel_case();
        let fn_trait_ident = Ident::new(&format!("{}_Fn", camel_name), Span::call_site());
        let evaluator_ident = Ident::new(&format!("{}_Evaluator", camel_name), Span::call_site());
        Ok(RpnFnGenerator {
            meta,
            item_fn,
            fn_trait_ident,
            evaluator_ident,
            arg_types,
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
        let (ctx_type, payload_type, result_type) = common_types();
        quote! {
            trait #fn_trait_ident #impl_generics #where_clause {
                fn eval(
                    self,
                    ctx: &mut #ctx_type,
                    payload: &#payload_type,
                ) -> #result_type ;
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
        let (ctx_type, payload_type, result_type) = common_types();
        quote! {
            impl #impl_generics #fn_trait_ident #ty_generics for #tp_ident #where_clause {
                default fn eval(
                    self,
                    ctx: &mut #ctx_type,
                    payload: &#payload_type,
                ) -> #result_type {
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
        let meta = &self.meta;
        let extract =
            (0..self.arg_types.len()).map(|i| Ident::new(&format!("arg{}", i), Span::call_site()));
        let call_arg = extract.clone();
        let ty_generics_turbofish = ty_generics.as_turbofish();
        let (ctx_type, payload_type, result_type) = common_types();
        quote! {
            impl #impl_generics #fn_trait_ident #ty_generics for #tp #where_clause {
                default fn eval(
                    self,
                    ctx: &mut #ctx_type,
                    payload: &#payload_type,
                ) -> #result_type {
                    let arg = &self;
                    let rows = payload.output_rows();
                    let mut result = Vec::with_capacity(rows);
                    for row in 0..rows {
                        #(let (#extract, arg) = arg.extract(row));*;
                        result.push( #fn_ident #ty_generics_turbofish ( #(#meta,)* #(#call_arg),* )?);
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
        let (ctx_type, payload_type, result_type) = common_types();
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
                    ctx: &mut #ctx_type,
                    payload: &#payload_type,
                ) -> #result_type {
                    #fn_trait_ident #ty_generics_turbofish::eval(def, ctx, payload)
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
        let arg_len = self.arg_types.len();
        for arg_index in 0..arg_len {
            evaluator = quote! { ArgConstructor::new(#arg_index, #evaluator) };
        }
        let fn_name = self.item_fn.ident.to_string();
        let (ctx_type, payload_type, result_type) = common_types();
        let rpn_fn_type = quote! { crate::coprocessor::dag::rpn_expr::function::RpnFnMeta };
        quote! {
            pub const fn #constructor_ident #impl_generics ()
            -> #rpn_fn_type #where_clause {
                #[inline]
                fn run #impl_generics (
                    ctx: &mut #ctx_type,
                    payload: &#payload_type,
                ) -> #result_type #where_clause {
                    use crate::coprocessor::dag::rpn_expr::function::{ArgConstructor, Evaluator, Null};
                    #evaluator.eval(Null, ctx, payload)
                }
                #rpn_fn_type {
                    name: #fn_name,
                    args_len: #arg_len,
                    fn_ptr: run #ty_generics_turbofish,
                }
            }
        }
    }
}

macro_rules! destruct {
    ($elem:expr, $tp:ident::$var:ident, $msg:expr, $span_elem:expr) => {
        if let $tp::$var(elem) = $elem {
            elem
        } else {
            return Err($span_elem.span().unwrap().error($msg));
        }
    };
    ($elem:expr, $tp:ident::$var:ident, $msg:expr) => {
        destruct!($elem, $tp::$var, $msg, $elem);
    };
}

fn parse_arg_type(arg: &FnArg) -> Result<TypePath> {
    let arg = destruct!(arg, FnArg::Captured, "Must be a captured parameter");
    let tp = destruct!(&arg.ty, Type::Reference, "Must be `&Option`");
    let path = destruct!(&*tp.elem, Type::Path, "Must be `Option`");
    let segments = &path.path.segments;
    let option = segments.iter().next().unwrap();
    if segments.len() != 1 || option.ident != "Option" {
        return Err(path.span().unwrap().error("Must be `Option`"));
    }
    let option_type = destruct!(
        &option.arguments,
        PathArguments::AngleBracketed,
        "Must be generic",
        option
    );
    if option_type.args.len() != 1 {
        return Err(option_type
            .span()
            .unwrap()
            .error("Option must has one type parameter"));
    }
    let eval_type = destruct!(
        option_type.args.iter().next().unwrap(),
        GenericArgument::Type,
        "Must be a type"
    );
    let eval_type = destruct!(eval_type, Type::Path, "Must be a concrete type");
    Ok(eval_type.clone())
}

/// Returns `TokenStream`s of some common types
fn common_types() -> (TokenStream, TokenStream, TokenStream) {
    (
        quote! { crate::coprocessor::dag::expr::EvalContext },
        quote! { crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload },
        quote! { crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_generic_fn() -> RpnFnGenerator {
        let item_fn = parse_str(
            r#"
            #[inline]
            fn foo(arg0: &Option<Int>, arg1: &Option<Real>) -> Result<Option<Decimal>> {
                Ok(None)
            }
        "#,
        )
        .unwrap();
        RpnFnGenerator::new(vec![], item_fn).unwrap()
    }

    #[test]
    fn test_no_generic_generate_fn_trait() {
        let gen = no_generic_fn();
        let expected: TokenStream = r#"
            trait Foo_Fn {
                fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    payload: &crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> ;
            }
        "#
        .parse()
        .unwrap();
        assert_eq!(expected.to_string(), gen.generate_fn_trait().to_string());
    }

    #[test]
    fn test_no_generic_generate_dummy_fn_trait_impl() {
        let gen = no_generic_fn();
        let expected: TokenStream = r#"
            impl<D_: crate::coprocessor::dag::rpn_expr::function::ArgDef> Foo_Fn for D_ {
                default fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    payload: &crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    panic!("Cannot apply {} on {:?}", "foo", self)
                }
            }
        "#
        .parse()
        .unwrap();
        assert_eq!(
            expected.to_string(),
            gen.generate_dummy_fn_trait_impl().to_string()
        );
    }

    #[test]
    fn test_no_generic_generate_real_fn_trait_impl() {
        let gen = no_generic_fn();
        let expected: TokenStream = r#"
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
                    payload: &crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    let arg = &self;
                    let rows = payload.output_rows();
                    let mut result = Vec::with_capacity(rows);
                    for row in 0..rows {
                        let (arg0, arg) = arg.extract(row);
                        let (arg1, arg) = arg.extract(row);
                        result.push(foo(arg0, arg1)?);
                    }
                    Ok(crate::coprocessor::codec::data_type::Evaluable::into_vector_value(result))
                }
            }
        "#
            .parse()
            .unwrap();
        assert_eq!(
            expected.to_string(),
            gen.generate_real_fn_trait_impl().to_string()
        );
    }

    #[test]
    fn test_no_generic_generate_evaluator() {
        let gen = no_generic_fn();
        let expected: TokenStream = r#"
            pub struct Foo_Evaluator ( std::marker::PhantomData <()> ) ;

            impl crate::coprocessor::dag::rpn_expr::function::Evaluator for Foo_Evaluator {
                #[inline]
                fn eval(
                    self,
                    def: impl crate::coprocessor::dag::rpn_expr::function::ArgDef,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    payload: &crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    Foo_Fn :: eval(def, ctx, payload)
                }
            }
        "#
        .parse()
        .unwrap();
        assert_eq!(expected.to_string(), gen.generate_evaluator().to_string());
    }

    #[test]
    fn test_no_generic_generate_constructor() {
        let gen = no_generic_fn();
        let expected: TokenStream = r#"
            pub const fn foo_fn_meta() -> crate::coprocessor::dag::rpn_expr::function::RpnFnMeta {
                #[inline]
                fn run(
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    payload: &crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    use crate::coprocessor::dag::rpn_expr::function::{ArgConstructor, Evaluator, Null};
                    ArgConstructor::new(
                        1usize,
                        ArgConstructor::new(
                            0usize,
                            Foo_Evaluator(std::marker::PhantomData)
                        )
                    ).eval(Null, ctx, payload)
                }
                crate::coprocessor::dag::rpn_expr::function::RpnFnMeta {
                    name: "foo",
                    args_len: 2usize,
                    fn_ptr: run,
                }
            }
        "#
            .parse()
            .unwrap();
        assert_eq!(expected.to_string(), gen.generate_constructor().to_string());
    }

    fn generic_fn() -> RpnFnGenerator {
        let item_fn = parse_str(
            r#"
            fn foo<A: M, B>(arg0: &Option<A::X>) -> Result<Option<B>>
            where B: N<M> {
                Ok(None)
            }
        "#,
        )
        .unwrap();
        RpnFnGenerator::new(vec![], item_fn).unwrap()
    }

    #[test]
    fn test_generic_generate_fn_trait() {
        let gen = generic_fn();
        let expected: TokenStream = r#"
            trait Foo_Fn<A: M, B>
            where B: N<M> {
                fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    payload: &crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> ;
            }
        "#
        .parse()
        .unwrap();
        assert_eq!(expected.to_string(), gen.generate_fn_trait().to_string());
    }

    #[test]
    fn test_generic_generate_dummy_fn_trait_impl() {
        let gen = generic_fn();
        let expected: TokenStream = r#"
            impl<
                A: M,
                B,
                D_: crate::coprocessor::dag::rpn_expr::function::ArgDef
            > Foo_Fn<A, B> for D_
            where B: N<M> {
                default fn eval(
                    self,
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    payload: &crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    panic!("Cannot apply {} on {:?}", "foo", self)
                }
            }
        "#
        .parse()
        .unwrap();
        assert_eq!(
            expected.to_string(),
            gen.generate_dummy_fn_trait_impl().to_string()
        );
    }

    #[test]
    fn test_generic_generate_real_fn_trait_impl() {
        let gen = generic_fn();
        let expected: TokenStream = r#"
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
                    payload: &crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    let arg = &self;
                    let rows = payload.output_rows();
                    let mut result = Vec::with_capacity(rows);
                    for row in 0..rows {
                        let (arg0, arg) = arg.extract(row);
                        result.push(foo :: <A, B> (arg0)?);
                    }
                    Ok(crate::coprocessor::codec::data_type::Evaluable::into_vector_value(result))
                }
            }
        "#
            .parse()
            .unwrap();
        assert_eq!(
            expected.to_string(),
            gen.generate_real_fn_trait_impl().to_string()
        );
    }

    #[test]
    fn test_generic_generate_evaluator() {
        let gen = generic_fn();
        let expected: TokenStream = r#"
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
                    payload: &crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue> {
                    Foo_Fn :: <A, B> :: eval(def, ctx, payload)
                }
            }
        "#
        .parse()
        .unwrap();
        assert_eq!(expected.to_string(), gen.generate_evaluator().to_string());
    }

    #[test]
    fn test_generic_generate_constructor() {
        let gen = generic_fn();
        let expected: TokenStream = r#"
            pub const fn foo_fn_meta <A: M, B> () -> crate::coprocessor::dag::rpn_expr::function::RpnFnMeta
            where B: N<M> {
                #[inline]
                fn run <A: M, B> (
                    ctx: &mut crate::coprocessor::dag::expr::EvalContext,
                    payload: &crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue>
                where B: N<M> {
                    use crate::coprocessor::dag::rpn_expr::function::{ArgConstructor, Evaluator, Null};
                    ArgConstructor::new(
                        0usize, 
                        Foo_Evaluator :: < A , B > (std::marker::PhantomData)
                    ).eval(Null, ctx, payload)
                }
                crate::coprocessor::dag::rpn_expr::function::RpnFnMeta {
                    name: "foo",
                    args_len: 1usize,
                    fn_ptr: run :: <A, B> ,
                }
            }
        "#
            .parse()
            .unwrap();
        assert_eq!(expected.to_string(), gen.generate_constructor().to_string());
    }
}

// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Implementation of the `rpn_fn` attribute macro.
//!
//! The macro should be applied to a function:
//!
//! ```ignore
//! #[rpn_fn]
//! fn foo(x: &Option<u32>) -> Result<Option<u8>> {
//!     Ok(None)
//! }
//! ```
//!
//! The expanded function implements an operation in the coprocessor.
//!
//! ## Arguments to macro
//!
//! If neither `varg` or `raw_varg` are supplied, then the generated arguments
//! follow from the supplied function's arguments. Each argument must have a type
//! `&Option<T>` for some `T`.
//!
//! ### `varg`
//!
//! The RPN operator takes a variable number of arguments. The arguments are passed
//! as a `&[&Option<T>]`. E.g.,
//!
//! ```ignore
//! #[rpn_fn(varg)]
//! pub fn foo(args: &[&Option<Int>]) -> Result<Option<Real>> {
//!     // Your RPN function logic
//! }
//! ```
//!
//! ### `raw_varg`
//!
//! The RPN operator takes a variable number of arguments. The arguments are passed
//! as a `&[ScalarValueRef]`. E.g.,
//!
//! ```ignore
//! #[rpn_fn(raw_varg)]
//! pub fn foo(args: &[ScalarValueRef<'_>]) -> Result<Option<Real>> {
//!     // Your RPN function logic
//! }
//! ```
//!
//! Use `raw_varg` where the function takes a variable number of arguments and the types
//! are not the same, for example, RPN function `case_when`.
//!
//! ### `max_args`
//!
//! The maximum number of arguments. The macro will generate code to check this
//! as part of validation. Only valid if `varg` or `raw_varg` is also used.
//! E.g., `#[rpn_fn(varg, max_args = 2)]`
//!
//! ### `min_args`
//!
//! The minimum number of arguments. The macro will generate code to check this
//! as part of validation. Only valid if `varg` or `raw_varg` is also used.
//! E.g., `#[rpn_fn(varg, min_args = 2)]`
//!
//! ### `extra_validator`
//!
//! A function name for custom validation code to be run when an operation is
//! validated. The validator function should have the signature `&tipb::Expr -> Result<()>`.
//! E.g., `#[rpn_fn(raw_varg, extra_validator = json_object_validator)]`
//!
//! ### `metadata_type`
//!
//! The type of the metadata structure defined in tipb.
//! If `metadata_mapper` is not specified, the protobuf metadata structure will be used as the metadata directly.
//!
//! ### `metadata_mapper`
//!
//! A function name to construct a new metadata or transform a protobuf metadata structure into a desired form.
//! The function signatures varies according to the existence of `metadata_mapper` and `metadata_type` as follows.
//!
//! - `metadata_mapper ` exists, `metadata_type` missing: `fn(&mut tipb::Expr) -> T`
//!
//! Constructs a new metadata in type `T`.
//!
//! - `metadata_mapper ` exists, `metadata_type` exists: `fn(MetaDataType, &mut tipb::Expr) -> T`
//!
//! Transforms a protobuf metadata type `MetaDataType` specified by `metadata_type` into a new type `T`.
//!
//! ### `capture`
//!
//! An array of argument names which are passed from the caller to the expanded
//! function. The argument names must be in scope in the generated `eval` or `run`
//! methods. Currently, that includes the following arguments (the supplied
//! function must accept these arguments with the corresponding types, in
//! addition to any other arguments):
//!
//! * `ctx: &mut expr::EvalContext`
//! * `output_rows: usize`
//! * `args: &[rpn_expr::RpnStackNode<'_>]`
//! * `extra: &mut rpn_expr::RpnFnCallExtra<'_>`
//! * `metadata: &T` (where T is the type returned by your metadata constructor)
//!
//! ```ignore
//! // This generates `with_context_fn_meta() -> RpnFnMeta`
//! #[rpn_fn(capture = [ctx])]
//! fn with_context(ctx: &mut EvalContext, param: &Option<Decimal>) -> Result<Option<Int>> {
//!     // Your RPN function logic
//! }
//! ```
//!
//! ## Generated code
//!
//! ### Vararg functions
//!
//! This includes `varg` and `raw_varg`.
//!
//! The supplied function is preserved and a constructor function is generated
//! with a `_fn_meta` suffix, e.g., `#[rpn_fn] fn foo ...` will preserve `foo` and
//! generate `foo_fn_meta`. The constructor function returns an `rpn_expr::RpnFnMeta`
//! value.
//!
//! The constructor function will include code for validating the runtime arguments
//! and running the function, pointers to these functions are stored in the result.
//!
//! ### Non-vararg functions
//!
//! Generate the following (examples assume a supplied function called `foo_bar`:
//!
//! * A trait to represent the function (`FooBar_Fn`) with a single function `eval`.
//!   - An impl of that trait for all argument types which panics
//!   - An impl of that trait for the supported argument type which calls the supplied function.
//! * An evaluator struct (`FooBar_Evaluator`) which implements `rpn_expr::function::Evaluator`,
//!   which includes an `eval` method which dispatches to `FooBar_Fn::eval`.
//! * A constructor function similar to the vararg case.
//!
//! The supplied function is preserved.
//!
//! The supported argument type is represented as a type-level list, for example, a
//! a function which takes two unsigned ints has an argument representation
//! something like `Arg<UInt, Arg<UInt, Null>>`. See documentation in
//! `components/tidb_query/src/rpn_expr/types/function.rs` for more details.
//!
//! The `_Fn` trait can be customised by implementing it manually.
//! For example, you are going to implement an RPN function called `regex_match` taking two
//! arguments, the regex and the string to match. You want to build the regex only once if the
//! first argument is a scalar. The code may look like:
//!
//! ```ignore
//! fn regex_match_impl(regex: &Regex, text: &Option<Bytes>) -> Result<Option<i32>> {
//!     // match text
//! }
//!
//! #[rpn_fn]
//! fn regex_match(regex: &Option<Bytes>, text: &Option<Bytes>) -> Result<Option<i32>> {
//!     let regex = build_regex(regex);
//!     regex_match_impl(&regex, text)
//! }
//!
//! // Pay attention that the first argument is specialized to `ScalarArg`
//! impl<'a, Arg1> RegexMatch_Fn for Arg<ScalarArg<'a, Bytes>, Arg<Arg1, Null>>
//! where Arg1: RpnFnArg<Type = &'a Option<Bytes>> {
//!     fn eval(
//!         self,
//!         ctx: &mut EvalContext,
//!         output_rows: usize,
//!         args: &[RpnStackNode<'_>],
//!         extra: &mut RpnFnCallExtra<'_>,
//!         metadata: &(dyn Any + Send),
//!     ) -> Result<VectorValue> {
//!         let (regex, arg) = self.extract(0);
//!         let regex = build_regex(regex);
//!         let mut result = Vec::with_capacity(output_rows);
//!         for row in 0..output_rows {
//!             let (text, _) = arg.extract(row);
//!             result.push(regex_match_impl(&regex, text)?);
//!         }
//!         Ok(Evaluable::into_vector_value(result))
//!     }
//! }
//! ```
//!
//! If the RPN function accepts variable number of arguments and all arguments have the same eval
//! type, like RPN function `coalesce`, you can use `#[rpn_fn(varg)]` like:
//!
//! ```ignore
//! #[rpn_fn(varg)]
//! pub fn foo(args: &[&Option<Int>]) -> Result<Option<Real>> {
//!     // Your RPN function logic
//! }
//! ```

use heck::CamelCase;
use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn::punctuated::Punctuated;
use syn::*;

/// Entry point for the `rpn_fn` attribute.
pub fn transform(attr: TokenStream, item_fn: TokenStream) -> Result<TokenStream> {
    let attr = parse2::<RpnFnAttr>(attr)?;
    let item_fn = parse2::<ItemFn>(item_fn)?;

    // FIXME: The macro cannot handle lifetime definitions now
    if let Some(lifetime) = item_fn.sig.generics.lifetimes().next() {
        return Err(Error::new_spanned(
            lifetime,
            "Lifetime definition is not allowed",
        ));
    }

    if attr.is_varg {
        Ok(VargsRpnFn::new(attr, item_fn)?.generate())
    } else if attr.is_raw_varg {
        Ok(RawVargsRpnFn::new(attr, item_fn)?.generate())
    } else {
        Ok(NormalRpnFn::new(attr, item_fn)?.generate())
    }
}

// ************** Parsing ******************************************************

mod kw {
    syn::custom_keyword!(Option);
}

/// Parses an attribute like `#[rpn_fn(varg, capture = [ctx, output_rows])`.
#[derive(Debug, Default)]
struct RpnFnAttr {
    /// Whether or not the function is a varg function. Varg function accepts `&[&Option<T>]`.
    is_varg: bool,

    /// Whether or not the function is a raw varg function. Raw varg function accepts `&[ScalarValueRef]`.
    is_raw_varg: bool,

    /// The maximum accepted arguments, which will be checked by the validator.
    ///
    /// Only varg or raw_varg function accepts a range of number of arguments. Other kind of
    /// function strictly stipulates number of arguments according to the function definition.
    max_args: Option<usize>,

    /// The minimal accepted arguments, which will be checked by the validator.
    min_args: Option<usize>,

    /// Extra validator.
    extra_validator: Option<TokenStream>,

    /// Metadata type.
    metadata_type: Option<TokenStream>,

    /// Metadata mapper.
    metadata_mapper: Option<TokenStream>,

    /// Special variables captured when calling the function.
    captures: Vec<Expr>,
}

impl parse::Parse for RpnFnAttr {
    fn parse(input: parse::ParseStream<'_>) -> Result<Self> {
        let mut is_varg = false;
        let mut is_raw_varg = false;
        let mut max_args = None;
        let mut min_args = None;
        let mut extra_validator = None;
        let mut metadata_type = None;
        let mut metadata_mapper = None;
        let mut captures = Vec::new();

        let config_items = Punctuated::<Expr, Token![,]>::parse_terminated(input).unwrap();
        for item in &config_items {
            match item {
                Expr::Assign(ExprAssign { left, right, .. }) => {
                    let left_str = format!("{}", left.into_token_stream());
                    match left_str.as_ref() {
                        "capture" => {
                            if let Expr::Array(ExprArray { elems, .. }) = &**right {
                                captures = elems.clone().into_iter().collect();
                            } else {
                                return Err(Error::new_spanned(
                                    right,
                                    "Expect array expression for `capture`",
                                ));
                            }
                        }
                        "min_args" => {
                            let lit: LitInt = parse2(right.into_token_stream()).map_err(|_| {
                                Error::new_spanned(right, "Expect int literal for `min_args`")
                            })?;
                            min_args = Some(lit.base10_parse()?);
                        }
                        "max_args" => {
                            let lit: LitInt = parse2(right.into_token_stream()).map_err(|_| {
                                Error::new_spanned(right, "Expect int literal for `max_args`")
                            })?;
                            max_args = Some(lit.base10_parse()?);
                        }
                        "extra_validator" => {
                            extra_validator = Some((&**right).into_token_stream());
                        }
                        "metadata_type" => {
                            metadata_type = Some((&**right).into_token_stream());
                        }
                        "metadata_mapper" => {
                            metadata_mapper = Some((&**right).into_token_stream());
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
                    let path_str = format!("{}", path.into_token_stream());
                    match path_str.as_ref() {
                        "varg" => {
                            is_varg = true;
                        }
                        "raw_varg" => {
                            is_raw_varg = true;
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
        if is_varg && is_raw_varg {
            return Err(Error::new_spanned(
                config_items,
                "`varg` and `raw_varg` conflicts to each other",
            ));
        }
        if !is_varg && !is_raw_varg && (min_args != None || max_args != None) {
            return Err(Error::new_spanned(
                config_items,
                "`min_args` or `max_args` is only available when `varg` or `raw_varg` presents",
            ));
        }

        Ok(Self {
            is_varg,
            is_raw_varg,
            max_args,
            min_args,
            extra_validator,
            metadata_type,
            metadata_mapper,
            captures,
        })
    }
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
            let result_type = segments.last().unwrap().clone();
            if let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) =
                result_type.arguments
            {
                let et = parse2::<RpnFnEvaluableType>(args.into_token_stream())?;
                Ok(Self {
                    eval_type: et.eval_type,
                })
            } else {
                Err(Error::new_spanned(
                    tp,
                    "expect angle bracketed path arguments",
                ))
            }
        } else {
            Err(Error::new_spanned(tp, "expect path"))
        }
    }
}

// ************** Code generation **********************************************

/// Helper utility to generate RPN function validator function.
struct ValidatorFnGenerator {
    tokens: Vec<TokenStream>,
}

impl ValidatorFnGenerator {
    fn new() -> Self {
        Self { tokens: Vec::new() }
    }

    fn validate_return_type(mut self, evaluable: &TypePath) -> Self {
        self.tokens.push(quote! {
            function::validate_expr_return_type(expr, #evaluable::EVAL_TYPE)?;
        });
        self
    }

    fn validate_max_args(mut self, max_args: Option<usize>) -> Self {
        if let Some(max_args) = max_args {
            self.tokens.push(quote! {
                function::validate_expr_arguments_lte(expr, #max_args)?;
            });
        }
        self
    }

    fn validate_min_args(mut self, min_args: Option<usize>) -> Self {
        if let Some(min_args) = min_args {
            self.tokens.push(quote! {
                function::validate_expr_arguments_gte(expr, #min_args)?;
            });
        }
        self
    }

    fn validate_args_identical_type(mut self, args_evaluable: &TypePath) -> Self {
        self.tokens.push(quote! {
            for child in expr.get_children() {
                function::validate_expr_return_type(child, #args_evaluable::EVAL_TYPE)?;
            }
        });
        self
    }

    fn validate_args_type(mut self, args_evaluables: &[TypePath]) -> Self {
        let args_len = args_evaluables.len();
        let args_n = 0..args_len;
        self.tokens.push(quote! {
            function::validate_expr_arguments_eq(expr, #args_len)?;
            let children = expr.get_children();
            #(
                function::validate_expr_return_type(
                    &children[#args_n],
                    #args_evaluables::EVAL_TYPE
                )?;
            )*
        });
        self
    }

    fn validate_by_fn(mut self, extra_validator: &Option<TokenStream>) -> Self {
        if let Some(ts) = extra_validator {
            self.tokens.push(quote! {
                #ts(expr)?;
            });
        }
        self
    }

    fn generate(
        self,
        impl_generics: &ImplGenerics<'_>,
        where_clause: Option<&WhereClause>,
    ) -> TokenStream {
        let inners = self.tokens;
        quote! {
            fn validate #impl_generics (
                expr: &tipb::Expr
            ) -> crate::Result<()> #where_clause {
                use crate::codec::data_type::Evaluable;
                use crate::rpn_expr::function;
                #( #inners )*
                Ok(())
            }
        }
    }
}

fn generate_init_metadata_fn(
    metadata_type: &Option<TokenStream>,
    metadata_mapper: &Option<TokenStream>,
    impl_generics: &ImplGenerics<'_>,
    where_clause: Option<&WhereClause>,
) -> TokenStream {
    let fn_body = match (metadata_type, metadata_mapper) {
        (Some(metadata_type), Some(metadata_mapper)) => quote! {
            crate::rpn_expr::types::function::extract_metadata_from_val::<#metadata_type>(expr.get_val())
                .and_then(|metadata| #metadata_mapper(expr, metadata))
                .map(|metadata| Box::new(metadata) as Box<(dyn std::any::Any + std::marker::Send + 'static)>)
        },
        (Some(metadata_type), None) => quote! {
            crate::rpn_expr::types::function::extract_metadata_from_val::<#metadata_type>(expr.get_val())
                .map_err(|e| other_err!("Decode metadata failed: {}", e))
                .map(|metadata| Box::new(metadata) as Box<(dyn std::any::Any + std::marker::Send + 'static)>)
        },
        (None, Some(metadata_mapper)) => quote! {
            #metadata_mapper(expr)
                .map(|metadata| Box::new(metadata) as Box<(dyn std::any::Any + std::marker::Send + 'static)>)
        },
        (None, None) => quote! { Ok(Box::new(())) },
    };
    quote! {
        fn init_metadata #impl_generics (expr: &mut ::tipb::Expr)
            -> Result<Box<dyn std::any::Any + Send>> #where_clause {
            #fn_body
        }
    }
}

fn generate_downcast_metadata(has_metadata: bool) -> TokenStream {
    if has_metadata {
        quote! {
            let metadata = std::any::Any::downcast_ref(metadata).expect("downcast metadata error");
        }
    } else {
        quote! {}
    }
}

fn generate_metadata_type_checker(
    metadata_type: &Option<TokenStream>,
    metadata_mapper: &Option<TokenStream>,
    impl_generics: &ImplGenerics<'_>,
    where_clause: Option<&WhereClause>,
    fn_body: TokenStream,
) -> TokenStream {
    if metadata_type.is_some() || metadata_mapper.is_some() {
        let metadata_expr = match (metadata_type, metadata_mapper) {
            (Some(_), Some(metadata_mapper)) => quote! {
                &#metadata_mapper(Default::default(), expr).unwrap()
            },
            (Some(_), None) => quote! { &Default::default() },
            (None, Some(metadata_mapper)) => quote! { &#metadata_mapper(expr).unwrap() },
            (None, None) => unreachable!(),
        };
        quote! {
            const _: () = {
                fn _type_checker #impl_generics (
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    expr: &mut ::tipb::Expr,
                ) #where_clause {
                    let metadata = #metadata_expr;
                    #fn_body
                }
            };
        }
    } else {
        quote! {}
    }
}

/// Generates a `varg` RPN fn.
#[derive(Debug)]
struct VargsRpnFn {
    captures: Vec<Expr>,
    max_args: Option<usize>,
    min_args: Option<usize>,
    extra_validator: Option<TokenStream>,
    metadata_type: Option<TokenStream>,
    metadata_mapper: Option<TokenStream>,
    item_fn: ItemFn,
    arg_type: TypePath,
    ret_type: TypePath,
}

impl VargsRpnFn {
    fn new(attr: RpnFnAttr, item_fn: ItemFn) -> Result<Self> {
        if item_fn.sig.inputs.len() != attr.captures.len() + 1 {
            return Err(Error::new_spanned(
                item_fn.sig.inputs,
                format!("Expect {} parameters", attr.captures.len() + 1),
            ));
        }

        let fn_arg = item_fn.sig.inputs.iter().nth(attr.captures.len()).unwrap();
        let arg_type =
            parse2::<VargsRpnFnSignatureParam>(fn_arg.into_token_stream()).map_err(|_| {
                Error::new_spanned(fn_arg, "Expect parameter type to be like `&[&Option<T>]`")
            })?;

        let ret_type = parse2::<RpnFnSignatureReturnType>(
            (&item_fn.sig.output).into_token_stream(),
        )
        .map_err(|_| {
            Error::new_spanned(
                &item_fn.sig.output,
                "Expect return type to be like `Result<Option<T>>`",
            )
        })?;
        Ok(Self {
            captures: attr.captures,
            max_args: attr.max_args,
            min_args: attr.min_args,
            extra_validator: attr.extra_validator,
            metadata_type: attr.metadata_type,
            metadata_mapper: attr.metadata_mapper,
            item_fn,
            arg_type: arg_type.eval_type,
            ret_type: ret_type.eval_type,
        })
    }

    fn generate(self) -> TokenStream {
        vec![
            self.generate_constructor(),
            self.item_fn.into_token_stream(),
        ]
        .into_iter()
        .collect()
    }

    fn generate_constructor(&self) -> TokenStream {
        let constructor_ident = Ident::new(
            &format!("{}_fn_meta", &self.item_fn.sig.ident),
            Span::call_site(),
        );
        let (impl_generics, ty_generics, where_clause) = self.item_fn.sig.generics.split_for_impl();
        let ty_generics_turbofish = ty_generics.as_turbofish();
        let fn_ident = &self.item_fn.sig.ident;
        let fn_name = self.item_fn.sig.ident.to_string();
        let arg_type = &self.arg_type;
        let captures = &self.captures;
        let init_metadata_fn = generate_init_metadata_fn(
            &self.metadata_type,
            &self.metadata_mapper,
            &impl_generics,
            where_clause,
        );
        let downcast_metadata = generate_downcast_metadata(
            self.metadata_type.is_some() || self.metadata_mapper.is_some(),
        );
        let metadata_type_checker = generate_metadata_type_checker(
            &self.metadata_type,
            &self.metadata_mapper,
            &impl_generics,
            where_clause,
            quote! {
                #fn_ident #ty_generics_turbofish ( #(#captures,)* &[]).ok();
            },
        );

        let validator_fn = ValidatorFnGenerator::new()
            .validate_return_type(&self.ret_type)
            .validate_max_args(self.max_args)
            .validate_min_args(self.min_args)
            .validate_args_identical_type(&self.arg_type)
            .validate_by_fn(&self.extra_validator)
            .generate(&impl_generics, where_clause);

        quote! {
            pub const fn #constructor_ident #impl_generics ()
            -> crate::rpn_expr::RpnFnMeta
            #where_clause
            {
                #[inline]
                fn run #impl_generics (
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> #where_clause {
                    #downcast_metadata
                    crate::rpn_expr::function::VARG_PARAM_BUF.with(|vargs_buf| {
                        use crate::codec::data_type::Evaluable;
                        let mut vargs_buf = vargs_buf.borrow_mut();
                        let args_len = args.len();
                        vargs_buf.resize(args_len, 0);
                        let mut result = Vec::with_capacity(output_rows);
                        for row_index in 0..output_rows {
                            for arg_index in 0..args_len {
                                let scalar_arg = args[arg_index].get_logical_scalar_ref(row_index);
                                let arg: &Option<#arg_type> = Evaluable::borrow_scalar_value_ref(&scalar_arg);
                                vargs_buf[arg_index] = arg as *const _ as usize;
                            }
                            result.push(#fn_ident #ty_generics_turbofish( #(#captures,)* unsafe {
                                &*(vargs_buf.as_slice() as *const _ as *const [&Option<#arg_type>])
                            })?);
                        }
                        Ok(Evaluable::into_vector_value(result))
                    })
                }

                #init_metadata_fn

                #metadata_type_checker

                #validator_fn

                crate::rpn_expr::RpnFnMeta {
                    name: #fn_name,
                    metadata_expr_ptr: init_metadata #ty_generics_turbofish,
                    validator_ptr: validate #ty_generics_turbofish,
                    fn_ptr: run #ty_generics_turbofish,
                }
            }
        }
    }
}

/// Generates a `raw_varg` RPN fn.
#[derive(Debug)]
struct RawVargsRpnFn {
    captures: Vec<Expr>,
    max_args: Option<usize>,
    min_args: Option<usize>,
    extra_validator: Option<TokenStream>,
    metadata_type: Option<TokenStream>,
    metadata_mapper: Option<TokenStream>,
    item_fn: ItemFn,
    ret_type: TypePath,
}

impl RawVargsRpnFn {
    fn new(attr: RpnFnAttr, item_fn: ItemFn) -> Result<Self> {
        if item_fn.sig.inputs.len() != attr.captures.len() + 1 {
            return Err(Error::new_spanned(
                item_fn.sig.inputs,
                format!("Expect {} parameters", attr.captures.len() + 1),
            ));
        }

        let ret_type = parse2::<RpnFnSignatureReturnType>(
            (&item_fn.sig.output).into_token_stream(),
        )
        .map_err(|_| {
            Error::new_spanned(
                &item_fn.sig.output,
                "Expect return type to be like `Result<Option<T>>`",
            )
        })?;
        Ok(Self {
            captures: attr.captures,
            max_args: attr.max_args,
            min_args: attr.min_args,
            extra_validator: attr.extra_validator,
            metadata_type: attr.metadata_type,
            metadata_mapper: attr.metadata_mapper,
            item_fn,
            ret_type: ret_type.eval_type,
        })
    }

    fn generate(self) -> TokenStream {
        vec![
            self.generate_constructor(),
            self.item_fn.into_token_stream(),
        ]
        .into_iter()
        .collect()
    }

    fn generate_constructor(&self) -> TokenStream {
        let constructor_ident = Ident::new(
            &format!("{}_fn_meta", &self.item_fn.sig.ident),
            Span::call_site(),
        );
        let (impl_generics, ty_generics, where_clause) = self.item_fn.sig.generics.split_for_impl();
        let ty_generics_turbofish = ty_generics.as_turbofish();
        let fn_ident = &self.item_fn.sig.ident;
        let fn_name = self.item_fn.sig.ident.to_string();
        let captures = &self.captures;
        let init_metadata_fn = generate_init_metadata_fn(
            &self.metadata_type,
            &self.metadata_mapper,
            &impl_generics,
            where_clause,
        );
        let downcast_metadata = generate_downcast_metadata(
            self.metadata_type.is_some() || self.metadata_mapper.is_some(),
        );
        let metadata_type_checker = generate_metadata_type_checker(
            &self.metadata_type,
            &self.metadata_mapper,
            &impl_generics,
            where_clause,
            quote! {
                #fn_ident #ty_generics_turbofish ( #(#captures,)* &[]).ok();
            },
        );

        let validator_fn = ValidatorFnGenerator::new()
            .validate_return_type(&self.ret_type)
            .validate_max_args(self.max_args)
            .validate_min_args(self.min_args)
            .validate_by_fn(&self.extra_validator)
            .generate(&impl_generics, where_clause);

        quote! {
            pub const fn #constructor_ident #impl_generics ()
            -> crate::rpn_expr::RpnFnMeta
            #where_clause
            {
                #[inline]
                fn run #impl_generics (
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> #where_clause {
                    #downcast_metadata
                    crate::rpn_expr::function::RAW_VARG_PARAM_BUF.with(|mut vargs_buf| {
                        let mut vargs_buf = vargs_buf.borrow_mut();
                        let args_len = args.len();
                        let mut result = Vec::with_capacity(output_rows);
                        for row_index in 0..output_rows {
                            vargs_buf.clear();
                            for arg_index in 0..args_len {
                                let scalar_arg = args[arg_index].get_logical_scalar_ref(row_index);
                                let scalar_arg = unsafe {
                                    std::mem::transmute::<ScalarValueRef<'_>, ScalarValueRef<'static>>(
                                        scalar_arg,
                                    )
                                };
                                vargs_buf.push(scalar_arg);
                            }
                            result.push(#fn_ident #ty_generics_turbofish( #(#captures,)* vargs_buf.as_slice())?);
                        }
                        Ok(Evaluable::into_vector_value(result))
                    })
                }

                #init_metadata_fn

                #metadata_type_checker

                #validator_fn

                crate::rpn_expr::RpnFnMeta {
                    name: #fn_name,
                    metadata_expr_ptr: init_metadata #ty_generics_turbofish,
                    validator_ptr: validate #ty_generics_turbofish,
                    fn_ptr: run #ty_generics_turbofish,
                }
            }
        }
    }
}

/// Generates an RPN fn which is neither `varg` or `raw_varg`.
#[derive(Debug)]
struct NormalRpnFn {
    captures: Vec<Expr>,
    extra_validator: Option<TokenStream>,
    metadata_type: Option<TokenStream>,
    metadata_mapper: Option<TokenStream>,
    item_fn: ItemFn,
    fn_trait_ident: Ident,
    evaluator_ident: Ident,
    arg_types: Vec<TypePath>,
    ret_type: TypePath,
}

impl NormalRpnFn {
    fn new(attr: RpnFnAttr, item_fn: ItemFn) -> Result<Self> {
        let mut arg_types = Vec::new();
        for fn_arg in item_fn.sig.inputs.iter().skip(attr.captures.len()) {
            let arg_type =
                parse2::<RpnFnSignatureParam>(fn_arg.into_token_stream()).map_err(|_| {
                    Error::new_spanned(fn_arg, "Expect parameter type to be like `&Option<T>`")
                })?;
            arg_types.push(arg_type.eval_type);
        }
        let ret_type = parse2::<RpnFnSignatureReturnType>(
            (&item_fn.sig.output).into_token_stream(),
        )
        .map_err(|_| {
            Error::new_spanned(
                &item_fn.sig.output,
                "Expect return type to be like `Result<Option<T>>`",
            )
        })?;
        let camel_name = item_fn.sig.ident.to_string().to_camel_case();
        let fn_trait_ident = Ident::new(&format!("{}_Fn", camel_name), Span::call_site());
        let evaluator_ident = Ident::new(&format!("{}_Evaluator", camel_name), Span::call_site());
        Ok(Self {
            captures: attr.captures,
            extra_validator: attr.extra_validator,
            metadata_type: attr.metadata_type,
            metadata_mapper: attr.metadata_mapper,
            item_fn,
            fn_trait_ident,
            evaluator_ident,
            arg_types,
            ret_type: ret_type.eval_type,
        })
    }

    fn generate(self) -> TokenStream {
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
        let (impl_generics, _, where_clause) = self.item_fn.sig.generics.split_for_impl();
        let fn_trait_ident = &self.fn_trait_ident;
        quote! {
            trait #fn_trait_ident #impl_generics #where_clause {
                fn eval(
                    self,
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue>;
            }
        }
    }

    fn generate_dummy_fn_trait_impl(&self) -> TokenStream {
        let mut generics = self.item_fn.sig.generics.clone();
        generics
            .params
            .push(parse_str("D_: crate::rpn_expr::function::ArgDef").unwrap());
        let fn_trait_ident = &self.fn_trait_ident;
        let tp_ident = Ident::new("D_", Span::call_site());
        let (_, ty_generics, _) = self.item_fn.sig.generics.split_for_impl();
        let (impl_generics, _, where_clause) = generics.split_for_impl();
        quote! {
            impl #impl_generics #fn_trait_ident #ty_generics for #tp_ident #where_clause {
                default fn eval(
                    self,
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> {
                    unreachable!()
                }
            }
        }
    }

    fn generate_real_fn_trait_impl(&self) -> TokenStream {
        let mut generics = self.item_fn.sig.generics.clone();
        generics
            .params
            .push(LifetimeDef::new(Lifetime::new("'arg_", Span::call_site())).into());
        let mut tp = quote! { crate::rpn_expr::function::Null };
        for (arg_index, arg_type) in self.arg_types.iter().enumerate().rev() {
            let arg_name = Ident::new(&format!("Arg{}_", arg_index), Span::call_site());
            let generic_param = quote! {
                #arg_name: crate::rpn_expr::function::RpnFnArg<
                    Type = &'arg_ Option<#arg_type>
                >
            };
            generics.params.push(parse2(generic_param).unwrap());
            tp = quote! { crate::rpn_expr::function::Arg<#arg_name, #tp> };
        }
        let fn_ident = &self.item_fn.sig.ident;
        let fn_trait_ident = &self.fn_trait_ident;
        let (_, ty_generics, _) = self.item_fn.sig.generics.split_for_impl();
        let (impl_generics, _, where_clause) = generics.split_for_impl();
        let captures = &self.captures;
        let extract =
            (0..self.arg_types.len()).map(|i| Ident::new(&format!("arg{}", i), Span::call_site()));
        let call_arg = extract.clone();
        let ty_generics_turbofish = ty_generics.as_turbofish();
        let downcast_metadata = generate_downcast_metadata(
            self.metadata_type.is_some() || self.metadata_mapper.is_some(),
        );
        let extract2 = extract.clone();
        let call_arg2 = extract.clone();
        let metadata_type_checker = generate_metadata_type_checker(
            &self.metadata_type,
            &self.metadata_mapper,
            &impl_generics,
            where_clause,
            quote! {
                let arg: &#tp = unsafe { &*std::ptr::null() };
                #(let (#extract2, arg) = arg.extract(0));*;
                #fn_ident #ty_generics_turbofish ( #(#captures,)* #(#call_arg2),* ).ok();
            },
        );

        quote! {
            impl #impl_generics #fn_trait_ident #ty_generics for #tp #where_clause {
                default fn eval(
                    self,
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> {
                    #downcast_metadata
                    let arg = &self;
                    let mut result = Vec::with_capacity(output_rows);
                    for row in 0..output_rows {
                        #(let (#extract, arg) = arg.extract(row));*;
                        result.push( #fn_ident #ty_generics_turbofish ( #(#captures,)* #(#call_arg),* )?);
                    }
                    Ok(crate::codec::data_type::Evaluable::into_vector_value(result))
                }
            }

            #metadata_type_checker
        }
    }

    fn generate_evaluator(&self) -> TokenStream {
        let generics = &self.item_fn.sig.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
        let evaluator_ident = &self.evaluator_ident;
        let fn_trait_ident = &self.fn_trait_ident;
        let ty_generics_turbofish = ty_generics.as_turbofish();
        let generic_types = generics.type_params().map(|type_param| &type_param.ident);

        quote! {
            pub struct #evaluator_ident #impl_generics (
                std::marker::PhantomData <(#(#generic_types),*)>
            ) #where_clause ;

            impl #impl_generics crate::rpn_expr::function::Evaluator
                for #evaluator_ident #ty_generics #where_clause {
                #[inline]
                fn eval(
                    self,
                    def: impl crate::rpn_expr::function::ArgDef,
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> {
                    #fn_trait_ident #ty_generics_turbofish::eval(def, ctx, output_rows, args, extra, metadata)
                }
            }
        }
    }

    fn generate_constructor(&self) -> TokenStream {
        let constructor_ident = Ident::new(
            &format!("{}_fn_meta", &self.item_fn.sig.ident),
            Span::call_site(),
        );
        let (impl_generics, ty_generics, where_clause) = self.item_fn.sig.generics.split_for_impl();
        let ty_generics_turbofish = ty_generics.as_turbofish();
        let evaluator_ident = &self.evaluator_ident;
        let mut evaluator =
            quote! { #evaluator_ident #ty_generics_turbofish (std::marker::PhantomData) };
        for (arg_index, arg_type) in self.arg_types.iter().enumerate() {
            evaluator = quote! { <ArgConstructor<#arg_type, _>>::new(#arg_index, #evaluator) };
        }
        let fn_name = self.item_fn.sig.ident.to_string();
        let init_metadata_fn = generate_init_metadata_fn(
            &self.metadata_type,
            &self.metadata_mapper,
            &impl_generics,
            where_clause,
        );

        let validator_fn = ValidatorFnGenerator::new()
            .validate_return_type(&self.ret_type)
            .validate_args_type(&self.arg_types)
            .validate_by_fn(&self.extra_validator)
            .generate(&impl_generics, where_clause);

        quote! {
            pub const fn #constructor_ident #impl_generics ()
            -> crate::rpn_expr::RpnFnMeta
            #where_clause
            {
                #[inline]
                fn run #impl_generics (
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> #where_clause {
                    use crate::rpn_expr::function::{ArgConstructor, Evaluator, Null};
                    #evaluator.eval(Null, ctx, output_rows, args, extra, metadata)
                }

                #init_metadata_fn

                #validator_fn

                crate::rpn_expr::RpnFnMeta {
                    name: #fn_name,
                    metadata_expr_ptr: init_metadata #ty_generics_turbofish,
                    validator_ptr: validate #ty_generics_turbofish,
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
            fn foo(arg0: &Option<Int>, arg1: &Option<Real>) -> crate::Result<Option<Decimal>> {
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
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue>;
            }
        };
        assert_eq!(expected.to_string(), gen.generate_fn_trait().to_string());
    }

    #[test]
    fn test_no_generic_generate_dummy_fn_trait_impl() {
        let gen = no_generic_fn();
        let expected: TokenStream = quote! {
            impl<D_: crate::rpn_expr::function::ArgDef> Foo_Fn for D_ {
                default fn eval(
                    self,
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> {
                    unreachable!()
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
                Arg1_: crate::rpn_expr::function::RpnFnArg<Type = &'arg_ Option<Real> > ,
                Arg0_: crate::rpn_expr::function::RpnFnArg<Type = &'arg_ Option<Int> >
            > Foo_Fn for crate::rpn_expr::function::Arg<
                Arg0_,
                crate::rpn_expr::function::Arg<
                    Arg1_,
                    crate::rpn_expr::function::Null
                >
            > {
                default fn eval(
                    self,
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> {
                    let arg = &self;
                    let mut result = Vec::with_capacity(output_rows);
                    for row in 0..output_rows {
                        let (arg0, arg) = arg.extract(row);
                        let (arg1, arg) = arg.extract(row);
                        result.push(foo(arg0, arg1)?);
                    }
                    Ok(crate::codec::data_type::Evaluable::into_vector_value(result))
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
            pub struct Foo_Evaluator(std::marker::PhantomData<()>);

            impl crate::rpn_expr::function::Evaluator for Foo_Evaluator {
                #[inline]
                fn eval(
                    self,
                    def: impl crate::rpn_expr::function::ArgDef,
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> {
                    Foo_Fn::eval(def, ctx, output_rows, args, extra, metadata)
                }
            }
        };
        assert_eq!(expected.to_string(), gen.generate_evaluator().to_string());
    }

    #[test]
    fn test_no_generic_generate_constructor() {
        let gen = no_generic_fn();
        let expected: TokenStream = quote! {
            pub const fn foo_fn_meta() -> crate::rpn_expr::RpnFnMeta {
                #[inline]
                fn run(
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> {
                    use crate::rpn_expr::function::{ArgConstructor, Evaluator, Null};
                    <ArgConstructor<Real, _>>::new(
                        1usize,
                        <ArgConstructor<Int, _>>::new(0usize, Foo_Evaluator(std::marker::PhantomData))
                    )
                    .eval(Null, ctx, output_rows, args, extra, metadata)
                }
                fn init_metadata(expr: &mut ::tipb::Expr)-> Result<Box<dyn std::any::Any + Send>>
                {
                    Ok(Box::new(()))
                }
                fn validate(expr: &tipb::Expr) -> crate::Result<()> {
                    use crate::codec::data_type::Evaluable;
                    use crate::rpn_expr::function;

                    function::validate_expr_return_type(expr, Decimal::EVAL_TYPE)?;
                    function::validate_expr_arguments_eq(expr, 2usize)?;
                    let children = expr.get_children();
                    function::validate_expr_return_type(&children[0usize], Int::EVAL_TYPE)?;
                    function::validate_expr_return_type(&children[1usize], Real::EVAL_TYPE)?;
                    Ok(())
                }
                crate::rpn_expr::RpnFnMeta {
                    name: "foo",
                    metadata_expr_ptr: init_metadata,
                    validator_ptr: validate,
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
            where B: N<A> {
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
            where
                B: N<A>
            {
                fn eval(
                    self,
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue>;
            }
        };
        assert_eq!(expected.to_string(), gen.generate_fn_trait().to_string());
    }

    #[test]
    fn test_generic_generate_dummy_fn_trait_impl() {
        let gen = generic_fn();
        let expected: TokenStream = quote! {
            impl<A: M, B, D_: crate::rpn_expr::function::ArgDef> Foo_Fn<A, B> for D_
            where
                B: N<A>
            {
                default fn eval(
                    self,
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> {
                    unreachable!()
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
                Arg0_: crate::rpn_expr::function::RpnFnArg<Type = &'arg_ Option<A::X> >
            > Foo_Fn<A, B> for crate::rpn_expr::function::Arg<
                Arg0_,
                crate::rpn_expr::function::Null
            > where B: N<A> {
                default fn eval(
                    self,
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> {
                    let arg = &self;
                    let mut result = Vec::with_capacity(output_rows);
                    for row in 0..output_rows {
                        let (arg0, arg) = arg.extract(row);
                        result.push(foo :: <A, B> (arg0)?);
                    }
                    Ok(crate::codec::data_type::Evaluable::into_vector_value(result))
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
            pub struct Foo_Evaluator<A: M, B>(std::marker::PhantomData<(A, B)>)
            where
                B: N<A>;

            impl<A: M, B> crate::rpn_expr::function::Evaluator for Foo_Evaluator<A, B>
            where
                B: N<A>
            {
                #[inline]
                fn eval(
                    self,
                    def: impl crate::rpn_expr::function::ArgDef,
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> {
                    Foo_Fn::<A, B>::eval(def, ctx, output_rows, args, extra, metadata)
                }
            }
        };
        assert_eq!(expected.to_string(), gen.generate_evaluator().to_string());
    }

    #[test]
    fn test_generic_generate_constructor() {
        let gen = generic_fn();
        let expected: TokenStream = quote! {
            pub const fn foo_fn_meta<A: M, B>() -> crate::rpn_expr::RpnFnMeta
            where
                B: N<A>
            {
                #[inline]
                fn run<A: M, B>(
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue>
                where
                    B: N<A>
                {
                    use crate::rpn_expr::function::{ArgConstructor, Evaluator, Null};
                    <ArgConstructor<A::X, _>>::new(0usize, Foo_Evaluator::<A, B>(std::marker::PhantomData))
                                .eval(Null, ctx, output_rows, args, extra, metadata)
                }
                fn init_metadata <A: M, B> (expr: &mut ::tipb::Expr)-> Result<Box<dyn std::any::Any + Send>>
                where
                    B: N<A>
                {
                    Ok(Box::new(()))
                }
                fn validate<A: M, B>(expr: &tipb::Expr) -> crate::Result<()>
                where
                    B: N<A>
                {
                    use crate::codec::data_type::Evaluable;
                    use crate::rpn_expr::function;
                    function::validate_expr_return_type(expr, B::EVAL_TYPE)?;
                    function::validate_expr_arguments_eq(expr, 1usize)?;
                    let children = expr.get_children();
                    function::validate_expr_return_type(&children[0usize], A::X::EVAL_TYPE)?;
                    Ok(())
                }
                crate::rpn_expr::RpnFnMeta {
                    name: "foo",
                    metadata_expr_ptr: init_metadata::<A, B>,
                    validator_ptr: validate::<A, B>,
                    fn_ptr: run::<A, B>,
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
                is_raw_varg: false,
                max_args: None,
                min_args: None,
                extra_validator: None,
                metadata_mapper: None,
                metadata_type: None,
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
                Arg1_: crate::rpn_expr::function::RpnFnArg<Type = &'arg_ Option<Real> > ,
                Arg0_: crate::rpn_expr::function::RpnFnArg<Type = &'arg_ Option<Int> >
            > Foo_Fn for crate::rpn_expr::function::Arg<
                Arg0_,
                crate::rpn_expr::function::Arg<
                    Arg1_,
                    crate::rpn_expr::function::Null
                >
            > {
                default fn eval(
                    self,
                    ctx: &mut crate::expr::EvalContext,
                    output_rows: usize,
                    args: &[crate::rpn_expr::RpnStackNode<'_>],
                    extra: &mut crate::rpn_expr::RpnFnCallExtra<'_>,
                    metadata: &(dyn std::any::Any + Send),
                ) -> crate::Result<crate::codec::data_type::VectorValue> {
                    let arg = &self;
                    let mut result = Vec::with_capacity(output_rows);
                    for row in 0..output_rows {
                        let (arg0, arg) = arg.extract(row);
                        let (arg1, arg) = arg.extract(row);
                        result.push(foo(ctx, arg0, arg1)?);
                    }
                    Ok(crate::codec::data_type::Evaluable::into_vector_value(result))
                }
            }
        };
        assert_eq!(
            expected.to_string(),
            gen.generate_real_fn_trait_impl().to_string()
        );
    }
}

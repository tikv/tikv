// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate quote;

use proc_macro2::{Group, TokenStream, TokenTree};
use quote::ToTokens;
use syn::{
    parse::{Parse, ParseStream, Result},
    punctuated::Punctuated,
    *,
};

/// This crate provides a macro that can be used to append a match expression with multiple
/// arms, where the tokens in the first arm, as a template, can be subsitituted and the template
/// arm will be expanded into multiple arms.
///
/// For example, the following code
///
/// ```ignore
/// match_template! {
///     T = [Int, Real, Double],
///     match Foo {
///         EvalType::T => { panic!("{}", EvalType::T); },
///         EvalType::Other => unreachable!(),
///     }
/// }
/// ```
///
/// generates
///
/// ```ignore
/// match Foo {
///     EvalType::Int => { panic!("{}", EvalType::Int); },
///     EvalType::Real => { panic!("{}", EvalType::Real); },
///     EvalType::Double => { panic!("{}", EvalType::Double); },
///     EvalType::Other => unreachable!(),
/// }
/// ```
///
/// In addition, substitution can vary on two sides of the arms.
///
/// For example,
///
/// ```ignore
/// match_template! {
///     T = [Foo, Bar => Baz],
///     match Foo {
///         EvalType::T => { panic!("{}", EvalType::T); },
///     }
/// }
/// ```
///
/// generates
///
/// ```ignore
/// match Foo {
///     EvalType::Foo => { panic!("{}", EvalType::Foo); },
///     EvalType::Bar => { panic!("{}", EvalType::Baz); },
/// }
/// ```
///
/// Wildcard match arm is also supported (but there will be no substitution).
#[proc_macro]
pub fn match_template(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mt = parse_macro_input!(input as MatchTemplate);
    mt.expand().into()
}
struct MatchTemplate {
    template_ident: Ident,
    substitutes: Punctuated<Substitution, Token![,]>,
    match_exp: Box<Expr>,
    template_arm: Arm,
    remaining_arms: Vec<Arm>,
}

impl Parse for MatchTemplate {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let template_ident = input.parse()?;
        input.parse::<Token![=]>()?;
        let substitutes_tokens;
        bracketed!(substitutes_tokens in input);
        let substitutes =
            Punctuated::<Substitution, Token![,]>::parse_terminated(&substitutes_tokens)?;
        input.parse::<Token![,]>()?;
        let m: ExprMatch = input.parse()?;
        let mut arms = m.arms;
        arms.iter_mut().for_each(|arm| arm.comma = None);
        assert!(!arms.is_empty(), "Expect at least 1 match arm");
        let template_arm = arms.remove(0);
        assert!(template_arm.guard.is_none(), "Expect no match arm guard");

        Ok(Self {
            template_ident,
            substitutes,
            match_exp: m.expr,
            template_arm,
            remaining_arms: arms,
        })
    }
}

impl MatchTemplate {
    fn expand(self) -> TokenStream {
        let Self {
            template_ident,
            substitutes,
            match_exp,
            template_arm,
            remaining_arms,
        } = self;
        let match_arms = substitutes.into_iter().map(|substitute| {
            let mut arm = template_arm.clone();
            let (left_tokens, right_tokens) = match substitute {
                Substitution::Identical(ident) => {
                    (ident.clone().into_token_stream(), ident.into_token_stream())
                }
                Substitution::Map(left_ident, right_tokens) => {
                    (left_ident.into_token_stream(), right_tokens)
                }
            };
            arm.pat = replace_in_token_stream(arm.pat, &template_ident, &left_tokens);
            arm.body = replace_in_token_stream(arm.body, &template_ident, &right_tokens);
            arm
        });
        quote! {
            match #match_exp {
                #(#match_arms,)*
                #(#remaining_arms,)*
            }
        }
    }
}

#[derive(Debug)]
enum Substitution {
    Identical(Ident),
    Map(Ident, TokenStream),
}

impl Parse for Substitution {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let left_ident = input.parse()?;
        let fat_arrow: Option<Token![=>]> = input.parse()?;
        if fat_arrow.is_some() {
            let mut right_tokens: Vec<TokenTree> = vec![];
            while !input.peek(Token![,]) && !input.is_empty() {
                right_tokens.push(input.parse()?);
            }
            Ok(Substitution::Map(
                left_ident,
                right_tokens.into_iter().collect(),
            ))
        } else {
            Ok(Substitution::Identical(left_ident))
        }
    }
}

fn replace_in_token_stream<T: ToTokens + Parse>(
    input: T,
    from_ident: &Ident,
    to_tokens: &TokenStream,
) -> T {
    let mut tokens = TokenStream::new();
    input.to_tokens(&mut tokens);

    let tokens: TokenStream = tokens
        .into_iter()
        .flat_map(|token| match token {
            TokenTree::Ident(ident) if ident == *from_ident => to_tokens.clone(),
            TokenTree::Group(group) => Group::new(
                group.delimiter(),
                replace_in_token_stream(group.stream(), from_ident, to_tokens),
            )
            .into_token_stream(),
            other => other.into(),
        })
        .collect();

    syn::parse2(tokens).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let input = r#"
            T = [Int, Real, Double],
            match foo() {
                EvalType::T => { panic!("{}", EvalType::T); },
                EvalType::Other => unreachable!(),
            }
        "#;

        let expect_output = r#"
            match foo() {
                EvalType::Int => { panic!("{}", EvalType::Int); },
                EvalType::Real => { panic!("{}", EvalType::Real); },
                EvalType::Double => { panic!("{}", EvalType::Double); },
                EvalType::Other => unreachable!(),
            }
        "#;
        let expect_output_stream: TokenStream = expect_output.parse().unwrap();

        let mt: MatchTemplate = syn::parse_str(input).unwrap();
        let output = mt.expand();
        assert_eq!(output.to_string(), expect_output_stream.to_string());
    }

    #[test]
    fn test_wildcard() {
        let input = r#"
            TT = [Foo, Bar],
            match v {
                VectorValue::TT => EvalType::TT,
                _ => unreachable!(),
            }
        "#;

        let expect_output = r#"
            match v {
                VectorValue::Foo => EvalType::Foo,
                VectorValue::Bar => EvalType::Bar,
                _ => unreachable!(),
            }
        "#;
        let expect_output_stream: TokenStream = expect_output.parse().unwrap();

        let mt: MatchTemplate = syn::parse_str(input).unwrap();
        let output = mt.expand();
        assert_eq!(output.to_string(), expect_output_stream.to_string());
    }

    #[test]
    fn test_map() {
        let input = r#"
            TT = [Foo, Bar => Baz, Bark => <&'static Whooh>()],
            match v {
                VectorValue::TT => EvalType::TT,
                EvalType::Other => unreachable!(),
            }
        "#;

        let expect_output = r#"
            match v {
                VectorValue::Foo => EvalType::Foo,
                VectorValue::Bar => EvalType::Baz,
                VectorValue::Bark => EvalType:: < & 'static Whooh>(),
                EvalType::Other => unreachable!(),
            }
        "#;
        let expect_output_stream: TokenStream = expect_output.parse().unwrap();

        let mt: MatchTemplate = syn::parse_str(input).unwrap();
        let output = mt.expand();
        assert_eq!(output.to_string(), expect_output_stream.to_string());
    }
}

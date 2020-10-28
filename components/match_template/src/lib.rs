// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate provides a macro that can be used to append a match expression with multiple
//! arms, where the tokens in the first arm, as a template, can be subsitituted and the template
//! arm will be expanded into multiple arms.
//!
//! For example, the following code
//!
//! ```ignore
//! match_template! {
//!     T = [Int, Real, Double],
//!     match Foo {
//!         EvalType::T => { panic!("{}", EvalType::T); },
//!         EvalType::Other => unreachable!(),
//!     }
//! }
//! ```
//!
//! generates
//!
//! ```ignore
//! match Foo {
//!     EvalType::Int => { panic!("{}", EvalType::Int); },
//!     EvalType::Real => { panic!("{}", EvalType::Real); },
//!     EvalType::Double => { panic!("{}", EvalType::Double); },
//!     EvalType::Other => unreachable!(),
//! }
//! ```
//!
//! In addition, substitution can vary on two sides of the arms.
//!
//! For example,
//!
//! ```ignore
//! match_template! {
//!     T = [Foo, Bar => Baz],
//!     match Foo {
//!         EvalType::T => { panic!("{}", EvalType::T); },
//!     }
//! }
//! ```
//!
//! generates
//!
//! ```ignore
//! match Foo {
//!     EvalType::Foo => { panic!("{}", EvalType::Foo); },
//!     EvalType::Bar => { panic!("{}", EvalType::Baz); },
//! }
//! ```
//!
//! Wildcard match arm is also supported (but there will be no substitution).

#[macro_use]
extern crate quote;
extern crate proc_macro;

use proc_macro2::{TokenStream, TokenTree};
use syn::fold::Fold;
use syn::parse::{Parse, ParseStream, Result};
use syn::punctuated::Punctuated;
use syn::*;

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
            let mut folder = match substitute {
                Substitution::Identical(ident) => MatchArmIdentFolder {
                    template_ident: template_ident.clone(),
                    left_ident: ident.clone(),
                    right_ident: ident,
                },
                Substitution::Map(left_ident, right_ident) => MatchArmIdentFolder {
                    template_ident: template_ident.clone(),
                    left_ident,
                    right_ident,
                },
            };
            folder.fold_arm(template_arm.clone())
        });
        quote! {
            match #match_exp {
                #(#match_arms,)*
                #(#remaining_arms,)*
            }
        }
    }
}

enum Substitution {
    Identical(Ident),
    Map(Ident, Ident),
}

impl Parse for Substitution {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let first_ident = input.parse()?;
        let fat_arrow: Option<Token![=>]> = input.parse()?;
        if fat_arrow.is_some() {
            let second_ident = input.parse()?;
            Ok(Substitution::Map(first_ident, second_ident))
        } else {
            Ok(Substitution::Identical(first_ident))
        }
    }
}

struct MatchArmIdentFolder {
    template_ident: Ident,
    left_ident: Ident,
    right_ident: Ident,
}

impl Fold for MatchArmIdentFolder {
    fn fold_pat(&mut self, i: Pat) -> Pat {
        ReplaceIdentFolder {
            from_ident: self.template_ident.clone(),
            to_ident: self.left_ident.clone(),
        }
        .fold_pat(i)
    }

    fn fold_expr(&mut self, i: Expr) -> Expr {
        ReplaceIdentFolder {
            from_ident: self.template_ident.clone(),
            to_ident: self.right_ident.clone(),
        }
        .fold_expr(i)
    }
}

struct ReplaceIdentFolder {
    from_ident: Ident,
    to_ident: Ident,
}

impl Fold for ReplaceIdentFolder {
    fn fold_macro(&mut self, i: Macro) -> Macro {
        let mut m = syn::fold::fold_macro(self, i);
        m.tokens = m
            .tokens
            .into_iter()
            .map(|tt| {
                if let TokenTree::Ident(i) = &tt {
                    if i == &self.from_ident {
                        return TokenTree::Ident(self.to_ident.clone());
                    }
                }
                tt
            })
            .collect();
        m
    }

    fn fold_ident(&mut self, i: Ident) -> Ident {
        if i == self.from_ident {
            self.to_ident.clone()
        } else {
            i
        }
    }
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
            TT = [Foo, Bar => Baz],
            match v {
                VectorValue::TT => EvalType::TT,
                EvalType::Other => unreachable!(),
            }
        "#;

        let expect_output = r#"
            match v {
                VectorValue::Foo => EvalType::Foo,
                VectorValue::Bar => EvalType::Baz,
                EvalType::Other => unreachable!(),
            }
        "#;
        let expect_output_stream: TokenStream = expect_output.parse().unwrap();

        let mt: MatchTemplate = syn::parse_str(input).unwrap();
        let output = mt.expand();
        assert_eq!(output.to_string(), expect_output_stream.to_string());
    }
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Procedural macros used for case conversion.

use proc_macro::{Group, Literal, TokenStream, TokenTree};

macro_rules! transform_idents_in_stream_to_string {
    ($stream:ident, $transform:expr) => {
        $stream
            .into_iter()
            .map(|token_tree| match token_tree {
                TokenTree::Ident(ref ident) => {
                    Literal::string(&$transform(ident.to_string())).into()
                }
                // find all idents in `TokenGroup` apply and reconstruct the group
                TokenTree::Group(ref group) => TokenTree::Group(Group::new(
                    group.delimiter(),
                    group
                        .stream()
                        .into_iter()
                        .map(|group_token_tree| {
                            if let TokenTree::Ident(ref ident) = group_token_tree {
                                Literal::string(&$transform(ident.to_string())).into()
                            } else {
                                group_token_tree
                            }
                        })
                        .collect::<TokenStream>(),
                )),
                _ => token_tree,
            })
            .collect()
    };
}

fn to_kebab(s: &str) -> String {
    to_snake(s).replace('_', "-")
}

fn to_snake(s: &str) -> String {
    let mut snake = String::new();
    for (i, ch) in s.char_indices() {
        if i > 0 && ch.is_uppercase() {
            snake.push('_');
        }
        snake.push(ch.to_ascii_lowercase());
    }
    snake
}

/// Expands idents in the input stream as kebab-case string literal
/// Caller should make sure the input identifer is a valid camel-case identifer
/// e.g. `HelloWorld` -> `hello-world`
#[proc_macro]
pub fn kebab_case(stream: TokenStream) -> TokenStream {
    transform_idents_in_stream_to_string!(stream, &|s: String| to_kebab(&s))
}

/// Expands idents in the input stream as snake-case string literal
/// Caller should make sure the input identifer is a valid camel-case identifer
/// e.g. `HelloWorld` -> `hello_world`
#[proc_macro]
pub fn snake_case(stream: TokenStream) -> TokenStream {
    transform_idents_in_stream_to_string!(stream, &|s: String| to_snake(&s))
}

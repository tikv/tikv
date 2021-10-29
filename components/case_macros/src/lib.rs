// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Procedural macros used for case conversion.
extern crate proc_macro;

use proc_macro::{Group, Ident, Literal, TokenStream, TokenTree};

fn transform_non_keyword_ident_to_string_liternal<Transform>(
    ident: &Ident,
    transform: Transform,
) -> Literal
where
    Transform: FnOnce(String) -> String,
{
    Literal::string(&transform(ident.to_string()))
}

macro_rules! transform_idents_to_string_liternal_in_stream {
    ($stream:ident, $transform:expr) => {
        $stream
            .into_iter()
            .map(|token_tree| match token_tree {
                TokenTree::Ident(ref ident) => {
                    transform_non_keyword_ident_to_string_liternal(ident, $transform).into()
                }
                // find all idents in `TokenGroup` apply and reconstruct the group
                TokenTree::Group(ref group) => TokenTree::Group(Group::new(
                    group.delimiter(),
                    group
                        .stream()
                        .into_iter()
                        .map(|group_token_tree| {
                            if let TokenTree::Ident(ref ident) = group_token_tree {
                                transform_non_keyword_ident_to_string_liternal(ident, $transform)
                                    .into()
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

fn to_kebab(enum_name: &str) -> String {
    let mut snake = String::new();
    for (i, ch) in enum_name.char_indices() {
        if i > 0 && ch.is_uppercase() {
            snake.push('_');
        }
        snake.push(ch.to_ascii_lowercase());
    }
    snake.replace('_', "-")
}

/// Expands idents in the input stream as kebab-case string literal
/// Caller should make sure the input identifer is a valid camel-case identifer
/// e.g. `HelloWorld` -> `hello-world`
#[proc_macro]
pub fn camel_to_kebab(stream: TokenStream) -> TokenStream {
    transform_idents_to_string_liternal_in_stream!(stream, &|s: String| to_kebab(&s))
}

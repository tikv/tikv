// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use proc_macro::TokenStream;
use proc_macro2::{TokenStream as TokenStream2, TokenTree};
use quote::{quote, ToTokens};
use syn::{parse_macro_input, parse_quote, Ident, ItemFn, Path};

/// test_case generate test cases when suite creation method provided.
///
/// ex:
/// #[test_case(with_resolver_v1)]
/// #[test_case(with_resolver_v2)]
/// fn test_something() {
///     let mut suite =
/// super::SuiteBuilder::new_named("test_something").build(); }
///
/// It generates two test cases as following:
///
/// #[cfg(test)]
/// mod test_something {
///     #[test]
///     fn test_with_resolver_v1() {
///         let mut suite =
/// super::SuiteBuilder::new_named("test_something").with_resolver_v1().build();
///         ...
///     }
///
///     #[test]
///     fn test_with_resolver_v2() {
///         let mut suite =
/// super::SuiteBuilder::new_named("test_something").with_resolver_v2().build();
///         ...
///     }
/// }
#[proc_macro_attribute]
pub fn test_case(arg: TokenStream, input: TokenStream) -> TokenStream {
    let mut fn_item = parse_macro_input!(input as ItemFn);
    let mut test_cases = vec![TokenStream2::from(arg)];
    let mut attrs_to_remove = vec![];

    let legal_test_case_name: Path = parse_quote!(test_case);
    for (idx, attr) in fn_item.attrs.iter().enumerate() {
        if legal_test_case_name == attr.path {
            test_cases.push(attr.into_token_stream());
            attrs_to_remove.push(idx);
        }
    }

    for i in attrs_to_remove.into_iter().rev() {
        fn_item.attrs.swap_remove(i);
    }

    render_test_cases(test_cases, fn_item.clone())
}

fn render_test_cases(test_cases: Vec<TokenStream2>, fn_item: ItemFn) -> TokenStream {
    let mut rendered_test_cases: Vec<TokenStream2> = vec![];
    for case in test_cases {
        let mut item = fn_item.clone();

        // parse test case to get the method name we want to add.
        let method = parse_test_case(case);
        let test_name = format!("test_{}", method);
        let stmts = &mut item.block.stmts;

        // modify new suite build at the beginning of the test.
        for stmt in stmts.iter_mut() {
            if let syn::Stmt::Local(local) = stmt {
                if let Some((_, expr)) = &mut local.init {
                    if let syn::Expr::MethodCall(method_call) = &**expr {
                        if let Some(new_expr) = parse_method_call(&method, method_call) {
                            *expr = Box::new(new_expr);
                        }
                    }
                }
            }
        }
        item.attrs.insert(0, parse_quote! { #[test] });
        let method_name = Ident::new(&test_name, item.sig.ident.span());
        item.sig.ident = method_name;

        rendered_test_cases.push(item.to_token_stream());
    }

    let mod_name = fn_item.sig.ident;
    let output = quote! {
        #[cfg(test)]
        mod #mod_name {
            #[allow(unused_imports)]
            use super::*;

            #(#rendered_test_cases)*
        }
    };
    output.into()
}

// Parsing test case to get method name.
// There are two cases that need to be considered
// 1. the first token is Ident type
// 2. the first token is Punct type
//
// use the following case as an example
// #[test_case(with_resolver_v1)]
// #[test_case(with_resolver_v2)]
// fn test_something() {}
//
// The first case ( #[test_case(with_resolver_v1)] )
// will be passed to the proc-macro "test_case" as the first argument and the
// #[test_case(...)] will be stripped off automatically. So the first token is
// the Ident type, namely "with_resolver_v1".
//
// The other two cases are in the `attr` fileds of ItemFn, and
// #[test_case(...)] are untouched. So the first token is Punct type.
fn parse_test_case(test_case: TokenStream2) -> Ident {
    let mut iter = test_case.into_iter();
    match iter.next().unwrap() {
        // ex: with_resolver_v1
        TokenTree::Ident(package) => package,
        // ex: #[with_resolver_v2]
        TokenTree::Punct(_) => match iter.next().unwrap() {
            TokenTree::Group(group) => {
                let mut iter = group.stream().into_iter();
                iter.next();
                match iter.next().unwrap() {
                    TokenTree::Group(group) => {
                        let stream = group.stream();
                        return parse_test_case(stream);
                    }
                    _ => panic!("Invalid token stream"),
                }
            }
            _ => panic!("Invalid token stream"),
        },
        _ => panic!("Invalid token stream"),
    }
}

// Parseing the specific method call to TokenStream, this function will
// 1. check whether this method call is SuiteBuilder::new_named
// 2. insert #method() before the #build().
// For example if (method, origin_method) is
// (with_resolver_v1, SuiteBuilder::new_named("async_commit").node(3).build())
// then output will be
// SuiteBuilder::new_named("async_commit").node(3).with_resolver_v1().build()
fn parse_method_call(method: &Ident, origin_method: &syn::ExprMethodCall) -> Option<syn::Expr> {
    match &*origin_method.receiver {
        syn::Expr::MethodCall(m) => {
            // A method call, so it will change
            // super::SuiteBuilder::new_named("async_commit").node(3)
            // to
            // super::SuiteBuilder::new_named("async_commit").node(3).#method()
            parse_method_call(method, m).map(|_| parse_quote!( #m.#method()))
        }
        syn::Expr::Call(call) => {
            // A general call, so it will change
            // super::SuiteBuilder::new_named("basic")
            // to
            // super::SuiteBuilder::new_named("basic").#method()
            if let syn::Expr::Path(path) = &*call.func {
                // check the stmt is contains super::SuiteBuilder::new_named
                if path.path.segments.last().unwrap().ident == "new_named" {
                    Some(parse_quote!(#call.#method()))
                } else {
                    None
                }
            } else {
                None
            }
        }
        _ => None,
    }
    .map(|res: syn::Expr| {
        // attach .build() to the end.
        parse_quote!(#res.build())
    })
}

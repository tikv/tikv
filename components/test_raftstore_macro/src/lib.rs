// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use proc_macro::TokenStream;
use proc_macro2::{TokenStream as TokenStream2, TokenTree};
use quote::{quote, ToTokens};
use syn::{parse_macro_input, parse_quote, Ident, ItemFn, Path};

/// test_case generate test cases using cluster creation method provided.
/// It also import the package related util module, which means we should locate
/// methods using Cluster in the related util modules.
///
/// ex:
/// #[test_case(test_raftstore::new_node_cluster)]
/// #[test_case(test_raftstore::new_server_cluster)]
/// #[test_case(test_raftstore_v2::new_node_cluster)]
/// fn test_something() {
///     let cluster = new_cluster(...)
/// }
///
/// It generates three test cases as following:
///
/// #[cfg(test)]
/// mod test_something {
///     #[test]
///     fn test_raftstore_new_node_cluster() {
///         use test_raftstore::(util::*, new_node_cluster as new_cluster);
///         let mut cluster = new_cluster(0, 1);
///     }
///
///     #[test]
///     fn test_raftstore_new_server_cluster() {
///         use test_raftstore::(util::*, new_server_cluster as new_cluster);
///         let mut cluster = new_cluster(0, 1);
///     }
///
///     #[test]
///     fn test_raftstore_v2_new_server_cluster() {
///         use test_raftstore::(util::*, test_raftstore_v2 as new_cluster);
///         let mut cluster = new_cluster(0, 1);
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

        // Parse test case to get the package name and the method name
        let (package, method) = parse_test_case(case);
        let test_name = format!("{}_{}", package, method);
        // Insert a use statment at the beginning of the test,
        // ex: " use test_raftstore::new_node_cluster as new_cluster ", so we can use
        // new_cluster in all situations.
        item.block.stmts.insert(
            0,
            syn::parse(
                quote! {
                    use #package::{util::*, #method as new_cluster, Simulator};
                }
                .into(),
            )
            .unwrap(),
        );
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

// Parsing test case to get package name and method name.
// There are two cases that need to be considered
// 1. the first token is Ident type
// 2. the first token is Punct type
//
// use the following case as an example
// #[test_case(test_raftstore::new_node_cluster)]
// #[test_case(test_raftstore::new_server_cluster)]
// #[test_case(test_raftstore_v2::new_node_cluster)]
// fn test_something() {}
//
// The first case ( #[test_case(test_raftstore::new_node_cluster)] )
// will be passed to the proc-macro "test_case" as the first argument and the
// #[test_case(...)] will be stripped off automatically. So the first token is
// the Ident type, namely "test_raftstore".
//
// The other two cases are in the `attr` fileds of ItemFn, and
// #[test_case(...)] are untouched. So the first token is Punct type.
fn parse_test_case(test_case: TokenStream2) -> (Ident, Ident) {
    let mut iter = test_case.into_iter();
    let package = match iter.next().unwrap() {
        // ex: test_raftstore::new_node_cluster
        TokenTree::Ident(package) => package,
        // ex: #[test_raftstore::new_node_cluster]
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
    };
    // Skip two ':'
    iter.next();
    iter.next();
    let method = match iter.next().unwrap() {
        TokenTree::Ident(method) => method,
        _ => panic!("Invalid token stream"),
    };
    (package, method)
}

extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::{TokenStream as TokenStream2, TokenTree};
use quote::{quote, ToTokens};
use syn::{parse_macro_input, parse_quote, Ident, ItemFn, Path};

// Ex: parse test_raftstore::new_node_cluster(1, 3) to
// (test_raftstore, new_node_cluster, 1, 3)
fn parse_attr(attr: TokenStream2) -> (Ident, Ident) {
    let mut iter = attr.into_iter();
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
                        return parse_attr(stream);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    };
    iter.next();
    iter.next();
    let method = match iter.next().unwrap() {
        TokenTree::Ident(method) => method,
        _ => unreachable!(),
    };
    (package, method)
}

#[proc_macro_attribute]
pub fn test_test(attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut fn_item = parse_macro_input!(input as ItemFn);
    let mut test_cases = vec![TokenStream2::from(attr)];
    let mut attrs_to_remove = vec![];

    let legal_test_case_name: Path = parse_quote!(test_test);
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

fn render_test_cases(test_cases: Vec<TokenStream2>, mut fn_item: ItemFn) -> TokenStream {
    let mut rendered_test_cases: Vec<TokenStream2> = vec![];
    for case in test_cases {
        let mut item = fn_item.clone();
        let mut attrs = vec![];
        attrs.append(&mut item.attrs);

        let (package, method) = parse_attr(case);
        let test_name = format!("{}_{}", package.to_string(), method.to_string());
        item.block.stmts.insert(
            0,
            syn::parse(
                quote! {
                    // let mut cluster = #package::#method(#id, #count);
                    use #package::#method as new_cluster;
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

    let mod_name = fn_item.sig.ident.clone();
    fn_item.attrs.clear();

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

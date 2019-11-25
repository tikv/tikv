// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use ::darling::FromDeriveInput;
use quote::quote;
use syn::Token;

mod kw {
    syn::custom_keyword!(state);
}

struct AggrFunctionStateExpr(syn::Expr);

impl syn::parse::Parse for AggrFunctionStateExpr {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        input.parse::<kw::state>()?;
        input.parse::<Token![=]>()?;
        Ok(Self(input.parse()?))
    }
}

#[derive(FromDeriveInput, Debug)]
#[darling(forward_attrs(aggr_function))]
pub struct AggrFunctionOpts {
    ident: syn::Ident,
    generics: syn::Generics,
    attrs: Vec<syn::Attribute>,
}

impl AggrFunctionOpts {
    pub fn generate_tokens(self) -> proc_macro2::TokenStream {
        let state_expr = self
            .attrs
            .first()
            .expect("Expect #[aggr_function] attribute")
            .parse_args::<AggrFunctionStateExpr>()
            .expect("Expect syntax to be #[aggr_function(state = expr)]")
            .0;
        let ident = &self.ident;
        let name = ident.to_string();
        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();
        quote! {
            impl #impl_generics crate::aggr_fn::AggrFunction for #ident #ty_generics #where_clause {
                #[inline]
                fn name(&self) -> &'static str {
                    #name
                }

                #[inline]
                fn create_state(&self) -> Box<dyn crate::aggr_fn::AggrFunctionState> {
                    Box::new(#state_expr)
                }
            }
        }
    }
}

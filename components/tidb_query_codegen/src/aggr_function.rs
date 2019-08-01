// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod kw {
    syn::custom_keyword!(state);
}

struct AggrFunctionStateExpr(syn::Expr);

impl syn::parse::Parse for AggrFunctionStateExpr {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let content;
        parenthesized!(content in input);
        content.parse::<kw::state>()?;
        content.parse::<Token![=]>()?;
        Ok(Self(content.parse()?))
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
        assert_eq!(self.attrs.len(), 1, "Expect #[aggr_function] attribute");
        let attr_tts = self.attrs.into_iter().next().unwrap().tts;
        let state_expr = syn::parse2::<AggrFunctionStateExpr>(attr_tts)
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

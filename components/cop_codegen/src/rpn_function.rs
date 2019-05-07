// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[derive(FromDeriveInput, Debug)]
#[darling(attributes(rpn_function))]
pub struct RpnFunctionOpts {
    ident: syn::Ident,
    generics: syn::Generics,
    args: usize,
}

impl RpnFunctionOpts {
    pub fn generate_tokens(self) -> proc_macro2::TokenStream {
        let ident = &self.ident;
        let name = ident.to_string();
        let args = self.args;
        let helper_fn_name = match self.args {
            0 => "eval_0_arg",
            1 => "eval_1_arg",
            2 => "eval_2_args",
            3 => "eval_3_args",
            _ => panic!("Unsupported argument length {}", self.args),
        };
        let helper_fn_ident =
            proc_macro2::Ident::new(helper_fn_name, proc_macro2::Span::call_site());

        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();
        quote! {
            impl #impl_generics tikv_util::AssertCopy for #ident #ty_generics #where_clause {}

            impl #impl_generics crate::coprocessor::dag::rpn_expr::RpnFunction for #ident #ty_generics #where_clause {
                #[inline]
                fn name(&self) -> &'static str {
                    #name
                }

                #[inline]
                fn args_len(&self) -> usize {
                    #args
                }

                #[inline]
                fn eval(
                    &self,
                    rows: usize,
                    context: &mut crate::coprocessor::dag::expr::EvalContext,
                    payload: crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload<'_>,
                ) -> crate::coprocessor::Result<crate::coprocessor::codec::data_type::VectorValue>
                {
                    crate::coprocessor::dag::rpn_expr::function::Helper::#helper_fn_ident(
                        rows,
                        Self::call,
                        context,
                        payload,
                    )
                }

                #[inline]
                fn box_clone(&self) -> Box<dyn crate::coprocessor::dag::rpn_expr::RpnFunction> {
                    Box::new(*self)
                }
            }
        }
    }
}

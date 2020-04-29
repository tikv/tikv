#![feature(proc_macro_diagnostic)]
#![recursion_limit = "256"]

extern crate proc_macro;

use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn future01_fn_root(args: TokenStream, item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemFn);
    let args = syn::parse_macro_input!(args as syn::AttributeArgs);

    let syn::ItemFn {
        attrs,
        vis,
        block,
        sig,
        ..
    } = input;

    let syn::Signature {
        output: return_type,
        inputs: params,
        unsafety,
        asyncness,
        constness,
        abi,
        ident,
        generics:
            syn::Generics {
                params: gen_params,
                where_clause,
                ..
            },
        ..
    } = sig;

    let tag = if let Some(syn::NestedMeta::Lit(syn::Lit::Str(s))) = args.get(0) {
        s.clone()
    } else {
        syn::LitStr::new(&ident.to_string(), ident.span())
    };

    quote::quote!(
        #(#attrs) *
        #vis #constness #unsafety #asyncness #abi fn #ident<#gen_params>(#params) #return_type
        #where_clause
        {
            let (__span_tx, __span_rx) = crossbeam::channel::unbounded();
            let __span = tracer::new_span_root(#tag, __span_tx);
            let __g = __span.enter();

            {
                #block
            }.inspect(move |_| {
                let _spans = __span_rx.iter();

                let _bytes = tracer_pb::serialize(_spans);
                // avoid dead-code elimination
                let _bytes = unsafe {
                    let ret = std::ptr::read_volatile(&_bytes);
                    std::mem::forget(_bytes);
                    ret
                };

                // let _spans: Vec<_> = _spans.collect();
                // tracer::util::draw_stdout(_spans.collect());
            })
        }
    )
    .into()
}

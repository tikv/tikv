use proc_macro::Diagnostic;
use proc_macro2::TokenStream;
use quote::{quote, quote_spanned, ToTokens};
use syn::{
    punctuated::Punctuated, spanned::Spanned, visit_mut::VisitMut, Block, Expr, ExprAsync,
    ExprCall, FnArg, Ident, Item, ItemFn, LitStr, Pat, PatIdent, Path, ReturnType, Signature, Stmt,
    Token, Type, TypePath,
};

use crate::MaybeItemFnRef;

const IN_FRAME: &str = "in_frame";

/// Given an existing function, generate an instrumented version of that
/// function
pub(crate) fn gen_function<'a, B: ToTokens + 'a>(
    input: MaybeItemFnRef<'a, B>,
    instrumented_function_name: &str,
    self_type: Option<&TypePath>,
) -> proc_macro2::TokenStream {
    // these are needed ahead of time, as ItemFn contains the function body _and_
    // isn't representable inside a quote!/quote_spanned! macro
    // (Syn's ToTokens isn't implemented for ItemFn)
    let MaybeItemFnRef {
        attrs,
        vis,
        sig,
        block,
    } = input;

    let Signature {
        output,
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

    let (return_type, return_span) = if let ReturnType::Type(_, return_type) = &output {
        (erase_impl_trait(return_type), return_type.span())
    } else {
        // Point at function name if we don't have an explicit return type
        (syn::parse_quote! { () }, ident.span())
    };
    // Install a fake return statement as the first thing in the function
    // body, so that we eagerly infer that the return type is what we
    // declared in the async fn signature.
    // The `#[allow(..)]` is given because the return statement is
    // unreachable, but does affect inference, so it needs to be written
    // exactly that way for it to do its magic.
    let fake_return_edge = quote_spanned! {return_span=>
        #[allow(unreachable_code, clippy::all)]
        if false {
            let __backtrace_attr_fake_return: #return_type = loop {};
            return __backtrace_attr_fake_return;
        }
    };
    let block = quote! {
        {
            #fake_return_edge
            #block
        }
    };

    let body = gen_block(
        &block,
        params,
        asyncness.is_some(),
        instrumented_function_name,
        self_type,
    );
    let mut truncated_params = params.clone();
    erase_in_frame(&mut truncated_params);

    quote!(
        #(#attrs) *
        #vis #constness #unsafety #asyncness #abi fn #ident<#gen_params>(#truncated_params) #output
        #where_clause
        {
            #body
        }
    )
}

/// Instrument a block
/// Instrument a block
fn gen_block<B: ToTokens>(
    block: &B,
    params: &Punctuated<FnArg, Token![,]>,
    async_context: bool,
    instrumented_function_name: &str,
    _self_type: Option<&TypePath>,
) -> proc_macro2::TokenStream {
    let name = LitStr::new(instrumented_function_name, block.span());
    let to_trace = traced_args(params.iter());
    // Generate the instrumented function body.
    // If the function is an `async fn`, this will wrap it in an async block,
    // which register its args needed to be traced to the span name.
    // enter the span and then perform the rest of the body.
    if async_context {
        if to_trace.is_empty() {
            quote!(tikv_util::frame!(#name; async move { #block }).await)
        } else {
            let formatter = gen_traced_frame_formatter(instrumented_function_name, &to_trace);
            quote!(tikv_util::frame!(dyn #formatter; async move { #block }).await)
        }
    } else {
        quote_spanned!(block.span() => #block)
    }
}

fn traced_args<'a>(params: impl Iterator<Item = &'a FnArg>) -> Vec<&'a FnArg> {
    let mut to_trace = vec![];
    for p in params {
        let attrs = match p {
            FnArg::Receiver(rec) => &rec.attrs,
            FnArg::Typed(ty) => &ty.attrs,
        };
        let should_trace = attrs.iter().any(|attr| attr.meta.path().is_ident(IN_FRAME));
        if should_trace {
            to_trace.push(p);
        }
    }
    to_trace
}

fn gen_traced_frame_formatter(func_name: &str, to_trace: &[&FnArg]) -> proc_macro2::TokenStream {
    let mut format_str_segments = vec![];
    let mut format_args = Punctuated::<Box<dyn ToTokens>, Token![,]>::new();
    for arg in to_trace.iter() {
        match arg {
            FnArg::Typed(arg) => {
                if let Pat::Ident(id) = &*arg.pat {
                    format_str_segments.push(format!("{}={{}}", id.ident));
                    format_args.push(Box::new(id.ident.clone()));
                } else {
                    Diagnostic::spanned(
                        arg.span().unwrap(),
                        proc_macro::Level::Warning,
                        "#[in_frame] cannot trace pattern matching argument yet.",
                    )
                    .emit();
                }
            }
            FnArg::Receiver(rec) => {
                format_str_segments.push("self={}".to_owned());
                format_args.push(Box::new(rec.self_token));
            }
        }
    }
    let format_str = format!("{func_name}({})", format_str_segments.join(", "));
    quote!(format!(#format_str, #format_args))
}

/// The specific async code pattern that was detected
enum AsyncKind<'a> {
    /// Immediately-invoked async fn, as generated by `async-trait <= 0.1.43`:
    /// `async fn foo<...>(...) {...}; Box::pin(foo<...>(...))`
    Function(&'a ItemFn),
    /// A function returning an async (move) block, optionally `Box::pin`-ed,
    /// as generated by `async-trait >= 0.1.44`:
    /// `Box::pin(async move { ... })`
    Async {
        async_expr: &'a ExprAsync,
        pinned_box: bool,
    },
}

pub(crate) struct AsyncInfo<'block> {
    // statement that must be patched
    source_stmt: &'block Stmt,
    kind: AsyncKind<'block>,
    self_type: Option<TypePath>,
    input: &'block ItemFn,
}

impl<'block> AsyncInfo<'block> {
    /// Get the AST of the inner function we need to hook, if it looks like a
    /// manual future implementation.
    ///
    /// When we are given a function that returns a (pinned) future containing
    /// the user logic, it is that (pinned) future that needs to be
    /// instrumented. Were we to instrument its parent, we would only
    /// collect information regarding the allocation of that future, and not
    /// its own span of execution.
    ///
    /// We inspect the block of the function to find if it matches any of the
    /// following patterns:
    ///
    /// - Immediately-invoked async fn, as generated by `async-trait <= 0.1.43`:
    ///   `async fn foo<...>(...) {...}; Box::pin(foo<...>(...))`
    ///
    /// - A function returning an async (move) block, optionally `Box::pin`-ed,
    ///   as generated by `async-trait >= 0.1.44`: `Box::pin(async move { ...
    ///   })`
    ///
    /// We the return the statement that must be instrumented, along with some
    /// other information.
    /// 'gen_body' will then be able to use that information to instrument the
    /// proper function/future.
    ///
    /// (this follows the approach suggested in
    /// https://github.com/dtolnay/async-trait/issues/45#issuecomment-571245673)
    pub(crate) fn from_fn(input: &'block ItemFn) -> Option<Self> {
        // are we in an async context? If yes, this isn't a manual async-like pattern
        if input.sig.asyncness.is_some() {
            return None;
        }

        let block = &input.block;

        // list of async functions declared inside the block
        let inside_funs = block.stmts.iter().filter_map(|stmt| {
            if let Stmt::Item(Item::Fn(fun)) = &stmt {
                // If the function is async, this is a candidate
                if fun.sig.asyncness.is_some() {
                    return Some((stmt, fun));
                }
            }
            None
        });

        // last expression of the block: it determines the return value of the
        // block, this is quite likely a `Box::pin` statement or an async block
        let (last_expr_stmt, last_expr) = block.stmts.iter().rev().find_map(|stmt| {
            if let Stmt::Expr(expr, _semi) = stmt {
                Some((stmt, expr))
            } else {
                None
            }
        })?;

        // is the last expression an async block?
        if let Expr::Async(async_expr) = last_expr {
            return Some(AsyncInfo {
                source_stmt: last_expr_stmt,
                kind: AsyncKind::Async {
                    async_expr,
                    pinned_box: false,
                },
                self_type: None,
                input,
            });
        }

        // is the last expression a function call?
        let (outside_func, outside_args) = match last_expr {
            Expr::Call(ExprCall { func, args, .. }) => (func, args),
            _ => return None,
        };

        // is it a call to `Box::pin()`?
        let path = match outside_func.as_ref() {
            Expr::Path(path) => &path.path,
            _ => return None,
        };
        if !path_to_string(path).ends_with("Box::pin") {
            return None;
        }

        // Does the call take an argument? If it doesn't,
        // it's not gonna compile anyway, but that's no reason
        // to (try to) perform an out of bounds access
        if outside_args.is_empty() {
            return None;
        }

        // Is the argument to Box::pin an async block that
        // captures its arguments?
        if let Expr::Async(async_expr) = &outside_args[0] {
            return Some(AsyncInfo {
                source_stmt: last_expr_stmt,
                kind: AsyncKind::Async {
                    async_expr,
                    pinned_box: true,
                },
                self_type: None,
                input,
            });
        }

        // Is the argument to Box::pin a function call itself?
        let func = match &outside_args[0] {
            Expr::Call(ExprCall { func, .. }) => func,
            _ => return None,
        };

        // "stringify" the path of the function called
        let func_name = match **func {
            Expr::Path(ref func_path) => path_to_string(&func_path.path),
            _ => return None,
        };

        // Was that function defined inside of the current block?
        // If so, retrieve the statement where it was declared and the function itself
        let (stmt_func_declaration, func) = inside_funs
            .into_iter()
            .find(|(_, fun)| fun.sig.ident == func_name)?;

        // If "_self" is present as an argument, we store its type to be able to rewrite
        // "Self" (the parameter type) with the type of "_self"
        let mut self_type = None;
        for arg in &func.sig.inputs {
            if let FnArg::Typed(ty) = arg {
                if let Pat::Ident(PatIdent { ref ident, .. }) = *ty.pat {
                    if ident == "_self" {
                        let mut ty = *ty.ty.clone();
                        // extract the inner type if the argument is "&self" or "&mut self"
                        if let Type::Reference(syn::TypeReference { elem, .. }) = ty {
                            ty = *elem;
                        }

                        if let Type::Path(tp) = ty {
                            self_type = Some(tp);
                            break;
                        }
                    }
                }
            }
        }

        Some(AsyncInfo {
            source_stmt: stmt_func_declaration,
            kind: AsyncKind::Function(func),
            self_type,
            input,
        })
    }

    pub(crate) fn gen_async(self, instrumented_function_name: &str) -> proc_macro::TokenStream {
        // let's rewrite some statements!
        let mut out_stmts: Vec<TokenStream> = self
            .input
            .block
            .stmts
            .iter()
            .map(|stmt| stmt.to_token_stream())
            .collect();

        if let Some((iter, _stmt)) = self
            .input
            .block
            .stmts
            .iter()
            .enumerate()
            .find(|(_iter, stmt)| *stmt == self.source_stmt)
        {
            // instrument the future by rewriting the corresponding statement
            out_stmts[iter] = match self.kind {
                // `Box::pin(immediately_invoked_async_fn())`
                AsyncKind::Function(fun) => gen_function(
                    fun.into(),
                    instrumented_function_name,
                    self.self_type.as_ref(),
                ),
                // `async move { ... }`, optionally pinned
                AsyncKind::Async {
                    async_expr,
                    pinned_box,
                } => {
                    let instrumented_block = gen_block(
                        &async_expr.block,
                        &self.input.sig.inputs,
                        true,
                        instrumented_function_name,
                        None,
                    );
                    let async_attrs = &async_expr.attrs;
                    if pinned_box {
                        quote! {
                            Box::pin(#(#async_attrs) * async move { #instrumented_block })
                        }
                    } else {
                        quote! {
                            #(#async_attrs) * async move { #instrumented_block }
                        }
                    }
                }
            };
        }

        let vis = &self.input.vis;
        let mut sig = self.input.sig.clone();
        erase_in_frame(&mut sig.inputs);
        let attrs = &self.input.attrs;
        quote!(
            #(#attrs) *
            #vis #sig {
                #(#out_stmts) *
            }
        )
        .into()
    }
}

// Return a path as a String
fn path_to_string(path: &Path) -> String {
    use std::fmt::Write;
    // some heuristic to prevent too many allocations
    let mut res = String::with_capacity(path.segments.len() * 5);
    for i in 0..path.segments.len() {
        write!(&mut res, "{}", path.segments[i].ident)
            .expect("writing to a String should never fail");
        if i < path.segments.len() - 1 {
            res.push_str("::");
        }
    }
    res
}

/// A visitor struct to replace idents and types in some piece
/// of code (e.g. the "self" and "Self" tokens in user-supplied
/// fields expressions when the function is generated by an old
/// version of async-trait).
struct IdentAndTypesRenamer<'a> {
    types: Vec<(&'a str, TypePath)>,
    idents: Vec<(Ident, Ident)>,
}

impl<'a> VisitMut for IdentAndTypesRenamer<'a> {
    // we deliberately compare strings because we want to ignore the spans
    // If we apply clippy's lint, the behavior changes
    #[allow(clippy::cmp_owned)]
    fn visit_ident_mut(&mut self, id: &mut Ident) {
        for (old_ident, new_ident) in &self.idents {
            if id.to_string() == old_ident.to_string() {
                *id = new_ident.clone();
            }
        }
    }

    fn visit_type_mut(&mut self, ty: &mut Type) {
        for (type_name, new_type) in &self.types {
            if let Type::Path(TypePath { path, .. }) = ty {
                if path_to_string(path) == *type_name {
                    *ty = Type::Path(new_type.clone());
                }
            }
        }
    }
}

// A visitor struct that replace an async block by its patched version
struct AsyncTraitBlockReplacer<'a> {
    block: &'a Block,
    patched_block: Block,
}

impl<'a> VisitMut for AsyncTraitBlockReplacer<'a> {
    fn visit_block_mut(&mut self, i: &mut Block) {
        if i == self.block {
            *i = self.patched_block.clone();
        }
    }
}

// Replaces any `impl Trait` with `_` so it can be used as the type in
// a `let` statement's LHS.
struct ImplTraitEraser;

impl VisitMut for ImplTraitEraser {
    fn visit_type_mut(&mut self, t: &mut Type) {
        if let Type::ImplTrait(..) = t {
            *t = syn::TypeInfer {
                underscore_token: Token![_](t.span()),
            }
            .into();
        } else {
            syn::visit_mut::visit_type_mut(self, t);
        }
    }
}

fn erase_impl_trait(ty: &Type) -> Type {
    let mut ty = ty.clone();
    ImplTraitEraser.visit_type_mut(&mut ty);
    ty
}

fn erase_in_frame<S>(args: &mut Punctuated<FnArg, S>) {
    for arg in args {
        let attrs = match arg {
            FnArg::Receiver(rec) => &mut rec.attrs,
            FnArg::Typed(ty) => &mut ty.attrs,
        };
        attrs.retain(|attr| !attr.meta.path().is_ident(IN_FRAME))
    }
}

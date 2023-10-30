// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::RefCell, convert::Infallible, time::Duration};

use collections::HashMap;
use futures::channel::oneshot;
use futures_util::{future::BoxFuture, Future, FutureExt};
use tokio::runtime::Handle;
use tracing::{
    dispatcher, instrument,
    subscriber::{self, DefaultGuard},
};

use crate::{
    async_trace::{
        layer::CurrentStacksLayer,
        tree::{MaybeSpan, TreeVisit},
    },
    defer, frame, info, root,
};

struct CollectTree<'a>(&'a mut Vec<String>);

impl<'a> TreeVisit for CollectTree<'a> {
    type Error = Infallible;

    fn enter(
        &mut self,
        _tree: &indextree::Arena<tracing::span::Id>,
        _node: indextree::NodeId,
        span: MaybeSpan<'_>,
    ) -> Result<(), Infallible> {
        self.0.push(match span {
            MaybeSpan::Span(s) => s.name().to_owned(),
            MaybeSpan::Dropped(id) => format!("<dropped {}>", id.into_u64()),
        });
        Ok(())
    }

    fn leave(
        &mut self,
        _tree: &indextree::Arena<tracing::span::Id>,
        _node: indextree::NodeId,
    ) -> Result<(), Infallible> {
        self.0.push("<step_out>".to_owned());
        Ok(())
    }
}

fn collect_tree() -> HashMap<String, Vec<String>> {
    let mut res = HashMap::default();
    let layer =
        dispatcher::get_default(|d| d.downcast_ref::<CurrentStacksLayer>().unwrap().clone());
    for ent in layer.get_span_trees().iter() {
        let mut stack = vec![];
        ent.value()
            .lock()
            .unwrap()
            .traverse_with(CollectTree(&mut stack))
            .unwrap();
        res.insert(stack[0].clone(), stack);
    }
    res
}

#[allow(unused)]
fn debug_dump_current_tree() -> String {
    let layer =
        dispatcher::get_default(|d| d.downcast_ref::<CurrentStacksLayer>().unwrap().clone());
    layer.fmt_string()
}

#[instrument]
async fn exec() {
    let jh = tokio::spawn(pending());
    let mut sleeping = frame!(Box::pin(tokio::time::sleep(Duration::from_secs(1))));
    let (tx, rx) = oneshot::channel();
    tokio::task::yield_now().await;
    frame!("selecting"; futures::future::select_all(
        [
            &mut sleeping as &mut (dyn Future<Output = ()> + Unpin),
            &mut frame!(jh.map(|_| ())) as _,
            &mut frame!(Box::pin(foo(tx))) as _,
        ]
        .iter_mut(),
    ))
    .await;
    let tree = rx.await.unwrap();
    assert_eq!(
        tree["exec"],
        [
            "exec",
            "selecting",
            "Box::pin(tokio::time::sleep(Duration::from_secs(1)))",
            "<step_out>",
            "foo",
            "bar",
            "fiz",
            "<step_out>",
            "buz",
            "baz",
            "<step_out>",
            "<step_out>",
            "<step_out>",
            "<step_out>",
            "<step_out>",
            "<step_out>",
        ]
    );
    assert_eq!(tree["pending"], ["pending", "<step_out>"]);
    assert_eq!(tree.len(), 2);
    let s = tokio::spawn(root!(sleeping));
    tokio::spawn(root!("sleep_100ms"; tokio::time::sleep(Duration::from_millis(100))));
    tokio::task::yield_now().await;
    let tree = collect_tree();
    assert_eq!(tree["exec"], ["exec", "<step_out>"]);
    assert_eq!(tree["sleep_100ms"], ["sleep_100ms", "<step_out>"]);
    assert_eq!(
        tree["sleeping"],
        [
            "sleeping",
            "Box::pin(tokio::time::sleep(Duration::from_secs(1)))",
            "<step_out>",
            "<step_out>"
        ]
    );
    assert_eq!(tree["pending"], ["pending", "<step_out>"]);
    assert_eq!(tree.len(), 4);
    s.await.unwrap();
}

#[instrument]
async fn pending() {
    std::future::pending::<()>().await
}

#[instrument(fields(answer = 43))]
async fn foo(tx: oneshot::Sender<HashMap<String, Vec<String>>>) {
    bar(tx).await;
}

#[instrument]
async fn bar(tx: oneshot::Sender<HashMap<String, Vec<String>>>) {
    futures::join!(fiz(), buz(tx));
}

#[instrument(skip_all)]
async fn fiz() {
    tokio::task::yield_now().await;
}

#[instrument(skip_all)]
async fn buz(tx: oneshot::Sender<HashMap<String, Vec<String>>>) {
    baz(tx).await;
}

#[instrument(skip_all)]
async fn baz(tx: oneshot::Sender<HashMap<String, Vec<String>>>) {
    println!("{}", debug_dump_current_tree());
    let _ = tx.send(collect_tree());
}

#[instrument]
async fn spawn_inner() {
    defer! {{
        info!("spawn_inner exits.")
    }};
    tokio::task::yield_now().await;
    Handle::current().spawn(root!("sleep_1"; async {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }));
}

#[instrument(skip_all)]
async fn dummy(rx: oneshot::Receiver<()>) {
    let _ = rx.await;
}

#[instrument]
async fn join_many(max: usize) -> HashMap<String, Vec<String>> {
    let v = RefCell::new(vec![]);
    let futs = (0..max)
        .map(|i| {
            frame!("nest_root"; futures::future::join_all( (0..i).map(|_| {
                    let (tx, rx) = oneshot::channel();
                    v.borrow_mut().push(tx);
                    dummy(rx)
                })); i)
        })
        .collect::<Vec<_>>();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        println!("{}", debug_dump_current_tree());
        tx.send(collect_tree()).unwrap();
        drop(v);
    });
    futures::future::join_all(futs).await;
    rx.await.unwrap()
}

fn local_sub() -> DefaultGuard {
    use tracing_subscriber::prelude::*;
    let sub = CurrentStacksLayer::default();
    let layer = tracing_subscriber::registry().with(sub);
    subscriber::set_default(layer)
}

fn count_to_zero(n: usize, rx: oneshot::Receiver<()>) -> BoxFuture<'static, ()> {
    if n == 0 {
        Box::pin(rx.map(|_| ()))
    } else {
        Box::pin(frame!(default; count_to_zero(n - 1, rx); n))
    }
}

#[tokio::test]
async fn test_long_stack() {
    let _g = local_sub();
    let (tx, rx) = oneshot::channel();
    let jh = tokio::spawn(root!(count_to_zero(20, rx)));
    tokio::task::yield_now().await;
    println!("{}", debug_dump_current_tree());
    tx.send(()).unwrap();
    jh.await.unwrap();
}

#[tokio::test]
async fn test_spawn_inner() {
    let _g = local_sub();

    let jh = tokio::spawn(root!("spawn_inner"; spawn_inner()));
    jh.await.unwrap();
    tokio::task::yield_now().await;

    let tree = collect_tree();
    assert_eq!(tree["sleep_1"], ["sleep_1", "<step_out>"]);
    assert_eq!(tree.len(), 1);
}

#[tokio::test]
async fn test_complex_tree() {
    let _g = local_sub();

    exec().await;
}

fn make_join_many_result(n: usize) -> Vec<&'static str> {
    let mut res = vec!["join_many"];
    for i in 1..n {
        res.push("nest_root");
        for _ in 0..i {
            res.push("dummy");
            res.push("<step_out>");
        }
        res.push("<step_out>");
    }
    res.push("<step_out>");
    res
}

#[tokio::test]
async fn test_spawn_many() {
    let _g = local_sub();

    for i in 2..10 {
        let res = join_many(i).await;
        assert_eq!(res.len(), 1);
        assert_eq!(res["join_many"], make_join_many_result(i));
    }
}

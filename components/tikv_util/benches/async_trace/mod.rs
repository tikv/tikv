#![feature(test)]

extern crate test;

mod bench_async_trace {
    use std::cell::RefCell;

    use futures::channel::oneshot;
    use test::Bencher;
    use tikv_util::{frame, root};
    use tracing::instrument;

    #[instrument(skip_all)]
    async fn dummy(rx: oneshot::Receiver<()>) {
        let _ = rx.await;
    }

    #[instrument]
    async fn join_many(max: usize) {
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
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            drop(v);
        });
        futures::future::join_all(futs).await;
    }

    #[bench]
    // test bench_async_trace::bench_async_trace          ... bench:     391,057
    // ns/iter (+/- 194,287)
    fn bench_async_trace(b: &mut Bencher) {
        let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        tikv_util::async_trace::init();
        b.iter(|| {
            rt.block_on(futures::future::join_all(
                (4..12).map(|x| rt.spawn(root!(join_many(x)))),
            ));
        })
    }

    #[bench]
    // test bench_async_trace::bench_async_trace_baseline ... bench:     133,163
    // ns/iter (+/- 44,870)
    fn bench_async_trace_baseline(b: &mut Bencher) {
        let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        b.iter(|| {
            rt.block_on(futures::future::join_all(
                (4..12).map(|x| rt.spawn(root!(join_many(x)))),
            ));
        })
    }
}

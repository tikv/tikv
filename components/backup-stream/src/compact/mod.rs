pub mod compaction;
pub mod errors;
pub mod execute;
pub mod statistic;
pub mod storage;

#[cfg(test)]
mod test;

mod util {
    use std::{future::Future, task::Poll};

    pub struct Cooperate {
        work_count: usize,
        yield_every: usize,
    }

    pub struct CooperateYield(bool);

    impl Future for CooperateYield {
        type Output = ();

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            if self.0 {
                cx.waker().wake_by_ref();
                self.0 = false;
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }

    impl Cooperate {
        pub fn new(yield_every: usize) -> Self {
            Self {
                work_count: 0,
                yield_every,
            }
        }

        pub fn step(&mut self) -> CooperateYield {
            self.work_count += 1;
            if self.work_count > self.yield_every {
                self.work_count = 0;
                CooperateYield(true)
            } else {
                CooperateYield(false)
            }
        }
    }

    pub fn select_vec<'a, T, F>(v: &'a mut Vec<F>) -> impl Future<Output = T> + 'a
    where
        F: Future<Output = T> + Unpin + 'a,
    {
        use futures::FutureExt;

        futures::future::poll_fn(|cx| {
            for (idx, fut) in v.iter_mut().enumerate() {
                match fut.poll_unpin(cx) {
                    std::task::Poll::Ready(item) => {
                        let _ = v.swap_remove(idx);
                        return item.into();
                    }
                    std::task::Poll::Pending => continue,
                }
            }
            std::task::Poll::Pending
        })
    }

    pub struct ExecuteAllExt {
        pub max_concurrency: usize,
    }

    impl Default for ExecuteAllExt {
        fn default() -> Self {
            Self {
                max_concurrency: 16,
            }
        }
    }

    pub async fn execute_all_ext<T, F, E>(futs: Vec<F>, ext: ExecuteAllExt) -> Result<Vec<T>, E>
    where
        F: Future<Output = Result<T, E>> + Unpin,
    {
        let mut pending_futures = vec![];
        let mut result = Vec::with_capacity(futs.len());
        for fut in futs {
            pending_futures.push(fut);
            if pending_futures.len() >= ext.max_concurrency {
                result.push(select_vec(&mut pending_futures).await?);
            }
        }
        result.append(&mut futures::future::try_join_all(pending_futures.into_iter()).await?);
        Ok(result)
    }
}

pub mod errors;
pub mod storage;

pub mod compaction;

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
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            if self.0 {
                cx.waker().wake_by_ref();
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

        pub fn check(&mut self) -> CooperateYield {
            self.work_count += 1;
            if self.work_count > self.yield_every {
                self.work_count = 0;
                CooperateYield(true)
            } else {
                CooperateYield(false)
            }
        }
    }

    pub fn select_vec<'a, T, F: Future<Output = T> + Unpin + 'a>(
        v: &'a mut Vec<F>,
    ) -> impl Future<Output = T> + 'a {
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
}

use futures::sync::oneshot;
use futures::Future;
use futures03::prelude::*;
use kvproto::kvrpcpb::CommandPri;
use std::future::Future as StdFuture;
use tikv_util::future_pool::{self, FuturePool};
use yatp::pool::{Local, Runner};
use yatp::queue::Extras;
use yatp::task::future::{Runner as FutureRunner, TaskCell};
use yatp::Remote;

use crate::storage::kv::{destroy_tls_engine, set_tls_engine, Engine};

#[derive(Clone)]
pub enum ReadPool {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp(Remote<TaskCell>),
}

impl ReadPool {
    pub fn spawn<F>(&self, f: F, priority: CommandPri, task_id: u64) -> Result<(), ReadPoolError>
    where
        F: StdFuture<Output = ()> + Send + 'static,
    {
        match self {
            ReadPool::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => {
                let pool = match priority {
                    CommandPri::High => read_pool_high,
                    CommandPri::Normal => read_pool_normal,
                    CommandPri::Low => read_pool_low,
                };

                pool.spawn(move || Box::pin(f.never_error()).compat())?;
            }
            ReadPool::Yatp(remote) => {
                let fixed_level = match priority {
                    CommandPri::High => Some(0),
                    CommandPri::Normal => None,
                    CommandPri::Low => Some(2),
                };
                let extras = Extras::new_multilevel(task_id, fixed_level);
                let task_cell = TaskCell::new(f, extras);
                remote.spawn(task_cell);
            }
        }
        Ok(())
    }

    pub fn spawn_handle<F, T, E>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
    ) -> Result<impl Future<Item = T, Error = E>, ReadPoolError>
    where
        F: StdFuture<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<Result<T, E>>();
        self.spawn(
            async move {
                let _ = tx.send(f.await);
            },
            priority,
            task_id,
        )?;
        Ok(rx.then(|res| res.expect("tx is dropped by the thread pool")))
    }
}

#[derive(Clone)]
pub struct ReadPoolRunner<E: Engine> {
    engine: Option<E>,
    inner: FutureRunner,
}

impl<E: Engine> Runner for ReadPoolRunner<E> {
    type TaskCell = TaskCell;

    fn start(&mut self, local: &mut Local<Self::TaskCell>) {
        set_tls_engine(self.engine.take().unwrap());
        self.inner.start(local)
    }

    fn handle(&mut self, local: &mut Local<Self::TaskCell>, task_cell: Self::TaskCell) -> bool {
        self.inner.handle(local, task_cell)
    }

    fn pause(&mut self, local: &mut Local<Self::TaskCell>) -> bool {
        self.inner.pause(local)
    }

    fn resume(&mut self, local: &mut Local<Self::TaskCell>) {
        self.inner.resume(local)
    }

    fn end(&mut self, local: &mut Local<Self::TaskCell>) {
        self.inner.end(local);
        unsafe { destroy_tls_engine::<E>() }
    }
}

impl<E: Engine> ReadPoolRunner<E> {
    pub fn new(engine: E, inner: FutureRunner) -> Self {
        ReadPoolRunner {
            engine: Some(engine),
            inner,
        }
    }
}

impl From<Vec<FuturePool>> for ReadPool {
    fn from(mut v: Vec<FuturePool>) -> ReadPool {
        assert_eq!(v.len(), 3);
        let read_pool_high = v.remove(2);
        let read_pool_normal = v.remove(1);
        let read_pool_low = v.remove(0);
        ReadPool::FuturePools {
            read_pool_high,
            read_pool_normal,
            read_pool_low,
        }
    }
}

impl From<Remote<TaskCell>> for ReadPool {
    fn from(yatp_remote: Remote<TaskCell>) -> Self {
        ReadPool::Yatp(yatp_remote)
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum ReadPoolError {
        FuturePoolFull(err: future_pool::Full) {
            from()
            cause(err)
            description(err.description())
        }
    }
}

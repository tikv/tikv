// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::errors::ReadPoolError;
use super::future_pool::FuturePool;

use futures::sync::oneshot;
use futures::{future, Future};
use kvproto::kvrpcpb::CommandPri;
use prometheus::IntGauge;
use std::future::Future as StdFuture;
use yatp::queue::Extras;
use yatp::task::future::TaskCell;
use yatp::Remote;

pub enum ReadPool {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp {
        pool: yatp::ThreadPool<TaskCell>,
        running_tasks: IntGauge,
        max_tasks: usize,
    },
}

impl ReadPool {
    pub fn handle(&self) -> ReadPoolHandle {
        match self {
            ReadPool::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => ReadPoolHandle::FuturePools {
                read_pool_high: read_pool_high.clone(),
                read_pool_normal: read_pool_normal.clone(),
                read_pool_low: read_pool_low.clone(),
            },
            ReadPool::Yatp {
                pool,
                running_tasks,
                max_tasks,
            } => ReadPoolHandle::Yatp {
                remote: pool.remote().clone(),
                running_tasks: running_tasks.clone(),
                max_tasks: *max_tasks,
            },
        }
    }

    pub fn remote(&self) -> Option<Remote<TaskCell>> {
        match self {
            ReadPool::Yatp { pool, .. } => Some(pool.remote().clone()),
            ReadPool::FuturePools { .. } => None,
        }
    }
}

#[derive(Clone)]
pub enum ReadPoolHandle {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp {
        remote: Remote<TaskCell>,
        running_tasks: IntGauge,
        max_tasks: usize,
    },
}

impl ReadPoolHandle {
    pub fn spawn<F>(&self, f: F, priority: CommandPri, task_id: u64) -> Result<(), ReadPoolError>
    where
        F: StdFuture<Output = ()> + Send + 'static,
    {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => {
                let pool = match priority {
                    CommandPri::High => read_pool_high,
                    CommandPri::Normal => read_pool_normal,
                    CommandPri::Low => read_pool_low,
                };

                pool.spawn(f)?;
            }
            ReadPoolHandle::Yatp {
                remote,
                running_tasks,
                max_tasks,
            } => {
                let running_tasks = running_tasks.clone();
                // Note that the running task number limit is not strict.
                // If several tasks are spawned at the same time while the running task number
                // is close to the limit, they may all pass this check and the number of running
                // tasks may exceed the limit.
                if running_tasks.get() as usize >= *max_tasks {
                    return Err(ReadPoolError::UnifiedReadPoolFull);
                }

                let fixed_level = match priority {
                    CommandPri::High => Some(0),
                    CommandPri::Normal => None,
                    CommandPri::Low => Some(2),
                };
                let extras = Extras::new_multilevel(task_id, fixed_level);
                let task_cell = TaskCell::new(
                    async move {
                        running_tasks.inc();
                        f.await;
                        running_tasks.dec();
                    },
                    extras,
                );
                remote.spawn(task_cell);
            }
        }
        Ok(())
    }

    pub fn spawn_handle<F, T>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
    ) -> impl Future<Item = T, Error = ReadPoolError>
    where
        F: StdFuture<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<T>();
        let spawn_res = self.spawn(
            async move {
                let _ = tx.send(f.await);
            },
            priority,
            task_id,
        );
        if let Err(e) = spawn_res {
            future::Either::A(future::err(e))
        } else {
            future::Either::B(rx.map_err(ReadPoolError::from))
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

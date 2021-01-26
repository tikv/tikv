// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;
use std::result::Result;
use std::sync::mpsc::{self, Sender};
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::time::Duration;

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);

pub struct IntervalDriver {
    name: String,
    interval: Duration,
    handle: Option<JoinHandle<()>>,
    sender: Option<Sender<bool>>,
    tasks: Vec<Box<dyn IntervalTask>>,
}

impl IntervalDriver {
    pub fn new(name: &str) -> Self {
        IntervalDriver {
            name: name.to_owned(),
            interval: DEFAULT_FLUSH_INTERVAL,
            handle: None,
            sender: None,
            tasks: vec![],
        }
    }

    pub fn set_flush_interval(&mut self, interval: Duration) {
        self.interval = interval;
    }

    pub fn add_task<T: IntervalTask + 'static>(&mut self, task: T) {
        self.tasks.push(Box::new(task));
    }

    pub fn start(&mut self) -> Result<(), io::Error> {
        let interval = self.interval;
        let (tx, rx) = mpsc::channel();
        self.sender = Some(tx);
        let mut tasks = vec![];
        std::mem::swap(&mut tasks, &mut self.tasks);
        let h = ThreadBuilder::new()
            .name(self.name.clone())
            .spawn(move || {
                tikv_alloc::add_thread_memory_accessor();
                for task in &mut tasks {
                    task.init();
                }
                while let Err(mpsc::RecvTimeoutError::Timeout) = rx.recv_timeout(interval) {
                    for task in &mut tasks {
                        task.on_tick();
                    }
                }
                for task in &mut tasks {
                    task.stop();
                }
                tikv_alloc::remove_thread_memory_accessor();
            })?;
        self.handle = Some(h);
        Ok(())
    }

    pub fn stop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }
        drop(self.sender.take().unwrap());
        if let Err(e) = h.unwrap().join() {
            error!("join thread failed"; "err" => ?e, "name" => self.name.as_str());
        }
    }
}

pub trait IntervalTask: Send + Sync {
    fn init(&mut self) {}
    fn on_tick(&mut self);
    fn stop(&mut self) {}
}

// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::mpsc;

use crate::ResourceEvent;

#[async_trait::async_trait]
pub trait ResourcePublisher: Send + Sync {
    fn publish(&self, event: ResourceEvent);
}

#[derive(Clone)]
pub(crate) struct EventPublisher {
    pub(crate) sender: mpsc::Sender<ResourceEvent>,
}

impl EventPublisher {
    pub(crate) fn new(sender: mpsc::Sender<ResourceEvent>) -> Self {
        Self { sender }
    }
}

#[async_trait::async_trait]
impl ResourcePublisher for EventPublisher {
    fn publish(&self, event: ResourceEvent) {
        let _ = self.sender.send(event);
    }
}

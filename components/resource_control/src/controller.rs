// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread};

use tikv_util::{info, mpsc};

use crate::{
    Config, EventHub, EventPublisher, MAX_BATCH_SIZE, ReadLimiter, ResourceCollector,
    ResourceEvent, ResourceHub, ResourcePublisher, ResourceSubscriber, ResourceType, ResourceUsage,
};

#[derive(Clone)]
pub struct ResourceLimitController {
    pub(crate) publisher: Arc<dyn ResourcePublisher>,
    pub(crate) event_hub: EventHub,
    pub(crate) read_limiter: Option<ReadLimiter>,
    pub(crate) dir: String,
    pub(crate) for_test: bool,
}

impl ResourceLimitController {
    pub fn new(config: Config) -> Self {
        let usage = ResourceUsage::new(config.clone());
        let event_hub = EventHub::new(config, usage);
        let (sender, receiver) = mpsc::unbounded();
        let publisher = Arc::new(EventPublisher::new(sender));
        let event_hub_clone_1 = event_hub.clone();
        std::thread::spawn(move || {
            ResourceLimitController::recv_events(event_hub_clone_1, receiver);
        });
        Self {
            publisher,
            event_hub,
            read_limiter: None,
            dir: String::new(),
            for_test: false,
        }
    }

    pub fn get_enabled(&self) -> bool {
        self.event_hub.get_enabled()
    }

    pub fn get_dry_run(&self) -> bool {
        self.event_hub.get_dry_run()
    }

    pub fn get_debug(&self) -> bool {
        self.event_hub.get_debug()
    }

    pub fn set_read_limiter(&mut self, read_limiter: ReadLimiter) {
        self.read_limiter = Some(read_limiter)
    }

    pub fn get_read_limiter(&self) -> Option<ReadLimiter> {
        self.read_limiter.clone()
    }

    pub fn register_subscriber(
        &self,
        resource_type: ResourceType,
        subscriber: Box<dyn ResourceSubscriber>,
    ) {
        self.event_hub
            .register_subscriber(resource_type, subscriber);
    }

    pub fn publisher(&self) -> Arc<dyn ResourcePublisher> {
        self.publisher.clone()
    }

    pub fn stop(&self) {
        self.publisher.publish(ResourceEvent::Stop);
    }

    pub fn run(&self) {
        info!("resource control run");
        let controller = self.clone();
        let mut collector = ResourceCollector::new(self.dir.clone());
        collector.for_test(self.for_test);
        std::thread::spawn(move || {
            loop {
                thread::sleep(controller.event_hub.get_stats_interval());
                if controller.event_hub.get_stopped() {
                    info!("stop resource control event hub");
                    break;
                }
                if let Some(cpu_events) = collector.collect_cpu_events() {
                    for cpu_event in cpu_events {
                        controller.publisher.publish(cpu_event);
                    }
                }
                if let Some(disk_event) = collector.collect_disk_events() {
                    controller.publisher.publish(disk_event);
                }
                controller.publisher.publish(ResourceEvent::Tick);
            }
        });
    }

    fn recv_events(event_hub: EventHub, receiver: mpsc::Receiver<ResourceEvent>) {
        info!("resource control recv events");
        let mut events = Vec::new();
        while let Ok(event) = receiver.recv() {
            events.push(event);
            while let Ok(event) = receiver.try_recv() {
                events.push(event);
                if events.len() > MAX_BATCH_SIZE {
                    break;
                }
            }
            let stop = event_hub.on_events(&events);
            if stop {
                return;
            }
            events.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tikv_util::{
        config::{ReadableDuration, ReadableSize},
        info,
    };

    use super::*;
    use crate::*;

    #[tokio::test]
    async fn test_resource_controller() {
        test_util::init_log_for_test();
        let mut config = Config::default();
        config.enabled = true;
        config.dry_run = false;
        config.window_size = ReadableDuration(Duration::from_millis(100));
        config.stats_interval = ReadableDuration(Duration::from_millis(10));
        config.limiter_timeout = ReadableDuration(Duration::from_millis(50));
        config.limiter_stats_interval = ReadableDuration(Duration::from_millis(10));
        config.smoothing_factor = 0.9;
        config.active_quota_ratio = 0.75;
        config.max_read_cpu_ratio = 0.3;
        config.max_read_bytes_per_core_per_second = ReadableSize::mb(2);
        let mut controller = ResourceLimitController::new(config.clone());
        controller.for_test = true;
        controller.run();

        let read_limiter = ReadLimiter::new(config.clone());
        controller.set_read_limiter(read_limiter.clone());

        let read_subscriber = ReadSubscriber::new(read_limiter.clone(), config.clone());
        controller.register_subscriber(
            ResourceType::Cpu {
                cpu_type: CpuType::UnifiedReadPool,
            },
            Box::new(read_subscriber.clone()),
        );
        controller.register_subscriber(ResourceType::Read, Box::new(read_subscriber.clone()));

        let publisher = controller.publisher();

        publisher.publish(ResourceEvent::CollectMetric(Metric {
            resource: Resource::Cpu {
                cpu_type: CpuType::UnifiedReadPool,
                cpu_time: 10.0,
            },
            scope: Scope::Global,
            severity: Severity::Normal,
        }));
        tokio::time::sleep(config.stats_interval.0 * 2).await;
        publisher.publish(ResourceEvent::CollectMetric(Metric {
            resource: Resource::Cpu {
                cpu_type: CpuType::UnifiedReadPool,
                cpu_time: 1000.0,
            },
            scope: Scope::Global,
            severity: Severity::Normal,
        }));
        publisher.publish(ResourceEvent::CollectMetric(Metric {
            resource: Resource::Read { bytes: 600000 },
            scope: Scope::ResourceGroup {
                name: "42".to_string(),
            },
            severity: Severity::Normal,
        }));
        publisher.publish(ResourceEvent::CollectMetric(Metric {
            resource: Resource::Read { bytes: 300000 },
            scope: Scope::ResourceGroup {
                name: "41".to_string(),
            },
            severity: Severity::Normal,
        }));
        tokio::time::sleep(config.stats_interval.0 * 2).await;

        let usage = controller
            .event_hub
            .get_resource_group_usage("42", ResourceType::Read)
            .unwrap();
        info!("usage {:?} {}", usage.resource_type, usage.usages.total());
        if let Some(group_read_limiter) = read_limiter.get_limiter("41") {
            let speed_limit = group_read_limiter.speed_limit();
            group_read_limiter.consume(speed_limit.1 as usize);
            let wait_time = group_read_limiter.wait_time();
            info!(
                "resource_group_read_limiter 41 speed_limit req {} bytes {} wait time {:?}",
                speed_limit.0, speed_limit.1, wait_time
            );
        }
        if let Some(group_read_limiter) = read_limiter.get_limiter("42") {
            let speed_limit = group_read_limiter.speed_limit();
            group_read_limiter.consume((speed_limit.1 / 2.0) as usize);
            let wait_time = group_read_limiter.wait_time();
            info!(
                "resource_group_read_limiter 42 speed_limit req {} bytes {} wait time {:?}",
                speed_limit.0, speed_limit.1, wait_time
            );
        }
        publisher.publish(ResourceEvent::CollectMetric(Metric {
            resource: Resource::Read { bytes: 300000 },
            scope: Scope::ResourceGroup {
                name: "43".to_string(),
            },
            severity: Severity::Normal,
        }));
        tokio::time::sleep(config.stats_interval.0 * 2).await;
        if let Some(group_read_limiter) = read_limiter.get_limiter("43") {
            let speed_limit = group_read_limiter.speed_limit();
            group_read_limiter.consume((speed_limit.1 / 3.0) as usize);
            let wait_time = group_read_limiter.wait_time();
            info!(
                "resource_group_read_limiter 43 speed_limit cpu {} bytes {} wait time {:?}",
                speed_limit.0, speed_limit.1, wait_time
            );
        }
        assert!(read_limiter.get_limiter("41").is_some());
        assert!(read_limiter.get_limiter("42").is_some());
        assert!(read_limiter.get_limiter("43").is_some());
        tokio::time::sleep(config.window_size.0 + config.limiter_timeout.0).await;
        assert!(read_limiter.get_limiter("41").is_none());
        assert!(read_limiter.get_limiter("42").is_none());
        assert!(read_limiter.get_limiter("43").is_none());
    }
}

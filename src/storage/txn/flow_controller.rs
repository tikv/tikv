// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use super::{
    singleton_flow_controller::EngineFlowController, tablet_flow_controller::TabletFlowController,
};

pub enum FlowControlType {
    Singleton(EngineFlowController),
    Tablet(TabletFlowController),
}

pub struct FlowController {
    controller: FlowControlType,
}

macro_rules! flow_controller_fn {
    ($fn_name: ident, $region_id: ident, $type: ident) => {
        pub fn $fn_name(&self, $region_id: u64) -> $type {
            match self.controller {
                FlowControlType::Singleton(ref controller) => controller.$fn_name($region_id),
                FlowControlType::Tablet(ref controller) => controller.$fn_name($region_id),
            }
        }
    };
    ($fn_name: ident, $region_id: ident, $bytes: ident, $type: ident) => {
        pub fn $fn_name(&self, $region_id: u64, $bytes: usize) -> $type {
            match self.controller {
                FlowControlType::Singleton(ref controller) => {
                    controller.$fn_name($region_id, $bytes)
                }
                FlowControlType::Tablet(ref controller) => controller.$fn_name($region_id, $bytes),
            }
        }
    };
}

impl FlowController {
    pub fn new(controller: FlowControlType) -> Self {
        Self { controller }
    }

    flow_controller_fn!(should_drop, region_id, bool);
    #[cfg(test)]
    flow_controller_fn!(discard_ratio, region_id, f64);
    flow_controller_fn!(consume, region_id, bytes, Duration);
    #[cfg(test)]
    flow_controller_fn!(total_bytes_consumed, region_id, usize);
    flow_controller_fn!(is_unlimited, region_id, bool);

    pub fn unconsume(&self, region_id: u64, bytes: usize) {
        match self.controller {
            FlowControlType::Singleton(ref controller) => controller.unconsume(region_id, bytes),
            FlowControlType::Tablet(ref controller) => controller.unconsume(region_id, bytes),
        }
    }
    pub fn enable(&self, enable: bool) {
        match self.controller {
            FlowControlType::Singleton(ref controller) => controller.enable(enable),
            FlowControlType::Tablet(ref controller) => controller.enable(enable),
        }
    }

    pub fn enabled(&self) -> bool {
        match self.controller {
            FlowControlType::Singleton(ref controller) => controller.enabled(),
            FlowControlType::Tablet(ref controller) => controller.enabled(),
        }
    }

    #[cfg(test)]
    pub fn set_speed_limit(&self, region_id: u64, speed_limit: f64) {
        match self.controller {
            FlowControlType::Singleton(ref controller) => {
                controller.set_speed_limit(region_id, speed_limit)
            }
            FlowControlType::Tablet(ref controller) => {
                controller.set_speed_limit(region_id, speed_limit)
            }
        }
    }
}

// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
pub mod singleton_flow_controller;
pub mod tablet_flow_controller;

use std::time::Duration;

pub use singleton_flow_controller::EngineFlowController;
pub use tablet_flow_controller::TabletFlowController;

pub enum FlowController {
    Singleton(EngineFlowController),
    Tablet(TabletFlowController),
}

macro_rules! flow_controller_fn {
    ($fn_name: ident, $region_id: ident, $type: ident) => {
        pub fn $fn_name(&self, $region_id: u64) -> $type {
            match self {
                FlowController::Singleton(ref controller) => controller.$fn_name($region_id),
                FlowController::Tablet(ref controller) => controller.$fn_name($region_id),
            }
        }
    };
    ($fn_name: ident, $region_id: ident, $bytes: ident, $type: ident) => {
        pub fn $fn_name(&self, $region_id: u64, $bytes: usize) -> $type {
            match self {
                FlowController::Singleton(ref controller) => {
                    controller.$fn_name($region_id, $bytes)
                }
                FlowController::Tablet(ref controller) => controller.$fn_name($region_id, $bytes),
            }
        }
    };
}

impl FlowController {
    flow_controller_fn!(should_drop, region_id, bool);
    #[cfg(test)]
    flow_controller_fn!(discard_ratio, region_id, f64);
    flow_controller_fn!(consume, region_id, bytes, Duration);
    #[cfg(test)]
    flow_controller_fn!(total_bytes_consumed, region_id, usize);
    flow_controller_fn!(is_unlimited, region_id, bool);

    pub fn unconsume(&self, region_id: u64, bytes: usize) {
        match self {
            FlowController::Singleton(ref controller) => controller.unconsume(region_id, bytes),
            FlowController::Tablet(ref controller) => controller.unconsume(region_id, bytes),
        }
    }
    pub fn enable(&self, enable: bool) {
        match self {
            FlowController::Singleton(ref controller) => controller.enable(enable),
            FlowController::Tablet(ref controller) => controller.enable(enable),
        }
    }

    pub fn enabled(&self) -> bool {
        match self {
            FlowController::Singleton(ref controller) => controller.enabled(),
            FlowController::Tablet(ref controller) => controller.enabled(),
        }
    }

    #[cfg(test)]
    pub fn set_speed_limit(&self, region_id: u64, speed_limit: f64) {
        match self {
            FlowController::Singleton(ref controller) => {
                controller.set_speed_limit(region_id, speed_limit)
            }
            FlowController::Tablet(ref controller) => {
                controller.set_speed_limit(region_id, speed_limit)
            }
        }
    }
}

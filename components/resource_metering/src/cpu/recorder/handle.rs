// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::config::ConfigManagerBuilder;
use crate::cpu::recorder::CpuRecorder;

use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use lazy_static::lazy_static;

const RECORD_FREQUENCY: f64 = 99.0;

lazy_static! {
    static ref HANDLE: RecorderHandle = {
        let config = crate::config::Config::default();

        let pause = Arc::new(AtomicBool::new(config.enabled));
        let pause0 = pause.clone();
        let precision_ms = Arc::new(AtomicU64::new(config.precision.0.as_millis() as _));
        let precision_ms0 = precision_ms.clone();

        let join_handle = std::thread::Builder::new()
            .name("cpu-recorder".to_owned())
            .spawn(move || {
                let mut recorder = CpuRecorder::new(pause0, precision_ms0);

                loop {
                    recorder.handle_pause();
                    recorder.handle_collector_registration();
                    recorder.handle_thread_registration();
                    recorder.record();
                    recorder.may_advance_window();
                    recorder.may_shrink();

                    std::thread::sleep(Duration::from_micros(
                        (1_000.0 / RECORD_FREQUENCY * 1_000.0) as _,
                    ));
                }
            })
            .expect("Failed to create recorder thread");

        RecorderHandle {
            join_handle,
            pause,
            precision_ms,
        }
    };
}

pub struct RecorderHandle {
    join_handle: JoinHandle<()>,
    pause: Arc<AtomicBool>,
    precision_ms: Arc<AtomicU64>,
}

impl RecorderHandle {
    pub fn pause() {
        HANDLE.pause.store(true, SeqCst);
    }

    pub fn resume() {
        HANDLE.pause.store(false, SeqCst);
        HANDLE.join_handle.thread().unpark();
    }

    pub fn set_precision(value: Duration) {
        HANDLE.precision_ms.store(value.as_millis() as _, SeqCst);
    }

    pub fn register_config_change(cfg_mgr_builder: &mut ConfigManagerBuilder) {
        cfg_mgr_builder.on_change_enabled(|enabled| {
            if enabled {
                Self::resume();
            } else {
                Self::pause();
            }
        });

        cfg_mgr_builder.on_change_precision(|p| Self::set_precision(p.0));
    }
}

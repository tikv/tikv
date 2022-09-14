// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(not(any(target_os = "linux", feature = "bcc-iosnoop")))]
mod stub {
    use std::cell::Cell;

    use strum::EnumCount;

    use crate::{IoBytes, IoContext, IoType};

    pub fn init() -> Result<(), String> {
        Err("No I/O tracing tool available".to_owned())
    }

    thread_local! {
        static IO_CTX: Cell<IoContext> = Cell::new(IoContext::new(IoType::Other));
    }

    pub(crate) fn get_io_context() -> IoContext {
        IO_CTX.with(|ctx| ctx.get())
    }

    pub(crate) fn set_io_context(new_ctx: IoContext) {
        IO_CTX.with(|ctx| ctx.set(new_ctx));
    }

    pub fn fetch_io_bytes() -> [IoBytes; IoType::COUNT] {
        Default::default()
    }

    pub fn fetch_thread_io_bytes() -> IoBytes {
        Default::default()
    }
}
#[cfg(not(any(target_os = "linux", feature = "bcc-iosnoop")))]
pub use stub::*;

#[cfg(all(target_os = "linux", feature = "bcc-iosnoop"))]
mod biosnoop;
#[cfg(all(target_os = "linux", feature = "bcc-iosnoop"))]
pub use biosnoop::*;

#[cfg(all(target_os = "linux", not(feature = "bcc-iosnoop")))]
mod proc;
#[cfg(all(target_os = "linux", not(feature = "bcc-iosnoop")))]
pub use proc::*;

#[cfg(test)]
mod tests {
    use tikv_util::sys::thread::StdThreadBuildWrapper;

    use super::*;
    use crate::{IoContext, IoType};

    #[bench]
    fn bench_fetch_io_bytes(b: &mut test::Bencher) {
        let (tx, rx) = std::sync::mpsc::sync_channel(0);
        let _ths = (0..8)
            .map(|_| {
                let tx_clone = tx.clone();
                std::thread::Builder::new().spawn_wrapper(move || {
                    set_io_context(IoContext::new(IoType::ForegroundWrite));
                    tx_clone.send(()).unwrap();
                })
            })
            .collect::<Vec<_>>();
        b.iter(|| fetch_io_bytes());
        for _ in 0..8 {
            rx.recv().unwrap();
        }
    }

    #[bench]
    fn bench_set_io_type(b: &mut test::Bencher) {
        let (tx, rx) = std::sync::mpsc::sync_channel(0);
        let _ths = (0..8)
            .map(|_| {
                let tx_clone = tx.clone();
                std::thread::Builder::new().spawn_wrapper(move || {
                    set_io_context(IoContext::new(IoType::ForegroundWrite));
                    tx_clone.send(()).unwrap();
                })
            })
            .collect::<Vec<_>>();
        b.iter(|| {
            if get_io_context().io_type == IoType::ForegroundRead {
                set_io_context(IoContext::new(IoType::ForegroundWrite));
            } else {
                set_io_context(IoContext::new(IoType::ForegroundRead));
            }
        });
        for _ in 0..8 {
            rx.recv().unwrap();
        }
    }
}

// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(not(any(target_os = "linux", feature = "bcc-iosnoop")))]
mod stub {
    use std::cell::Cell;

    use strum::EnumCount;

    use crate::{IOBytes, IOContext, IOType};

    pub fn init() -> Result<(), String> {
        Err("No I/O tracing tool available".to_owned())
    }

    thread_local! {
        static IO_CTX: Cell<IOContext> = Cell::new(IOContext::new(IOType::Other));
    }

    pub(crate) fn get_io_context() -> IOContext {
        IO_CTX.with(|ctx| ctx.get())
    }

    pub(crate) fn set_io_context(new_ctx: IOContext) {
        IO_CTX.with(|ctx| ctx.set(new_ctx));
    }

    pub fn fetch_io_bytes() -> [IOBytes; IOType::COUNT] {
        Default::default()
    }

    pub fn fetch_thread_io_bytes() -> IOBytes {
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
    use super::*;
    use crate::{IOContext, IOType};

    #[bench]
    fn bench_fetch_io_bytes(b: &mut test::Bencher) {
        let (tx, rx) = std::sync::mpsc::sync_channel(0);
        let _ths = (0..8)
            .map(|_| {
                let tx_clone = tx.clone();
                std::thread::Builder::new().spawn(move || {
                    set_io_context(IOContext::new(IOType::ForegroundWrite));
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
                std::thread::Builder::new().spawn(move || {
                    set_io_context(IOContext::new(IOType::ForegroundWrite));
                    tx_clone.send(()).unwrap();
                })
            })
            .collect::<Vec<_>>();
        b.iter(|| {
            if get_io_context().io_type == IOType::ForegroundRead {
                set_io_context(IOContext::new(IOType::ForegroundWrite));
            } else {
                set_io_context(IOContext::new(IOType::ForegroundRead));
            }
        });
        for _ in 0..8 {
            rx.recv().unwrap();
        }
    }
}

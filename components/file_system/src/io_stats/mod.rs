// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(not(any(target_os = "linux", feature = "bcc-iosnoop")))]
mod stub {
    use std::cell::Cell;

    use strum::EnumCount;

    use crate::{IoBytes, IoType};

    pub fn init() -> Result<(), String> {
        Err("No I/O tracing tool available".to_owned())
    }

    thread_local! {
        static IO_TYPE: Cell<IoType> = const {Cell::new(IoType::Other)};
    }

    pub fn set_io_type(new_io_type: IoType) {
        IO_TYPE.with(|io_type| {
            io_type.set(new_io_type);
        });
    }

    pub fn get_io_type() -> IoType {
        IO_TYPE.with(|io_type| io_type.get())
    }

    pub fn fetch_io_bytes() -> [IoBytes; IoType::COUNT] {
        Default::default()
    }

    pub fn get_thread_io_bytes_total() -> Result<IoBytes, String> {
        Err("unimplemented".into())
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

// A struct assists testing IO stats.
//
// O_DIRECT requires I/O to be 512-byte aligned.
// See https://man7.org/linux/man-pages/man2/open.2.html#NOTES
#[cfg(test)]
#[repr(align(512))]
#[cfg_attr(not(target_os = "linux"), allow(unused))]
pub(crate) struct A512<const SZ: usize>(pub [u8; SZ]);

#[cfg(test)]
mod tests {
    use tikv_util::sys::thread::StdThreadBuildWrapper;

    use super::*;
    use crate::IoType;

    #[bench]
    fn bench_fetch_io_bytes(b: &mut test::Bencher) {
        let (tx, rx) = std::sync::mpsc::sync_channel(0);
        let _ths = (0..8)
            .map(|_| {
                let tx_clone = tx.clone();
                std::thread::Builder::new().spawn_wrapper(move || {
                    set_io_type(IoType::ForegroundWrite);
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
                    set_io_type(IoType::ForegroundWrite);
                    tx_clone.send(()).unwrap();
                })
            })
            .collect::<Vec<_>>();
        b.iter(|| match get_io_type() {
            IoType::ForegroundWrite => set_io_type(IoType::ForegroundRead),
            _ => set_io_type(IoType::ForegroundWrite),
        });
        for _ in 0..8 {
            rx.recv().unwrap();
        }
    }
}

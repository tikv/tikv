// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(not(any(target_os = "linux", feature = "bcc-iosnoop")))]
mod stub {
    use std::cell::Cell;

    use strum::EnumCount;

    use crate::{IOBytes, IOType};

    pub fn init() -> Result<(), String> {
        Err("No I/O tracing tool available".to_owned())
    }

    thread_local! {
        static IO_TYPE: Cell<IOType> = Cell::new(IOType::Other);
    }

    pub fn set_io_type(new_io_type: IOType) {
        IO_TYPE.with(|io_type| {
            io_type.set(new_io_type);
        });
    }

    pub fn get_io_type() -> IOType {
        IO_TYPE.with(|io_type| io_type.get())
    }

    pub fn fetch_io_bytes() -> [IOBytes; IOType::COUNT] {
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
    use crate::IOType;

    #[bench]
    fn bench_fetch_io_bytes(b: &mut test::Bencher) {
        let (tx, rx) = std::sync::mpsc::sync_channel(0);
        let _ths = (0..8)
            .map(|_| {
                let tx_clone = tx.clone();
                std::thread::Builder::new().spawn_wrapper(move || {
                    set_io_type(IOType::ForegroundWrite);
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
                    set_io_type(IOType::ForegroundWrite);
                    tx_clone.send(()).unwrap();
                })
            })
            .collect::<Vec<_>>();
        b.iter(|| match get_io_type() {
            IOType::ForegroundWrite => set_io_type(IOType::ForegroundRead),
            _ => set_io_type(IOType::ForegroundWrite),
        });
        for _ in 0..8 {
            rx.recv().unwrap();
        }
    }
}

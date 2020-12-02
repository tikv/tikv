// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use bcc::{table::Table, Kprobe, BPF};

use std::ptr;

static mut BPF_TABLE: Option<(BPF, Table)> = None;

unsafe fn set_bpf_table(bpf: BPF, table: Table) {
    BPF_TABLE = Some((bpf, table))
}

#[repr(C)]
#[derive(Default)]
pub struct IOStats {
    read: u64,
    write: u64,
}

fn parse_io_stats(x: &[u8]) -> IOStats {
    unsafe { ptr::read(x.as_ptr() as *const IOStats) }
}

unsafe fn get_io_stats_of_tid(tid: u32) -> Option<IOStats> {
    if let Some((_, t)) = BPF_TABLE.as_mut() {
        let val = t
            .get(&mut tid.to_ne_bytes())
            .map(|v| parse_io_stats(&v))
            .unwrap_or_default();
        Some(val)
    } else {
        None
    }
}

pub struct IOContext {
    tid: u32,
    read_bytes: u64,
    write_bytes: u64,
}

impl IOContext {
    pub fn new() -> Self {
        let tid = nix::unistd::gettid().as_raw() as u32;
        let stats = unsafe { get_io_stats_of_tid(tid) };
        IOContext {
            read_bytes: stats.as_ref().map(|s| s.read).unwrap_or(0),
            write_bytes: stats.as_ref().map(|s| s.write).unwrap_or(0),
            tid: tid,
        }
    }

    pub fn delta(&self) -> IOStats {
        let stats = unsafe { get_io_stats_of_tid(self.tid) };
        IOStats {
            read: stats.as_ref().map(|s| s.read).unwrap_or(0) - self.read_bytes,
            write: stats.as_ref().map(|s| s.write).unwrap_or(0) - self.write_bytes,
        }
    }

    pub fn delta_and_refresh(&mut self) -> IOStats {
        let stats = unsafe { get_io_stats_of_tid(self.tid) };
        let read = stats.as_ref().map(|s| s.read).unwrap_or(0);
        let write = stats.as_ref().map(|s| s.write).unwrap_or(0);
        let stats = IOStats {
            read: read - self.read_bytes,
            write: write - self.write_bytes,
        };
        self.read_bytes = read;
        self.write_bytes = write;
        stats
    }
}

pub fn init_io_snooper() -> Result<(), String> {
    let code = include_str!("biosnoop.c");
    let code = code.replace("##TGID##", &nix::unistd::getpid().to_string());
    // compile the above BPF code!
    let mut bpf = BPF::new(&code).map_err(|e| e.to_string())?;
    // attach kprobes
    Kprobe::new()
        .handler("trace_pid_start")
        .function("blk_account_io_start")
        .attach(&mut bpf)
        .map_err(|e| e.to_string())?;
    Kprobe::new()
        .handler("trace_req_completion")
        .function("blk_account_io_completion")
        .attach(&mut bpf)
        .map_err(|e| e.to_string())?;
    // the "events" table is where the "open file" events get sent
    let table = bpf.table("statsbypid").map_err(|e| e.to_string())?;
    unsafe {
        set_bpf_table(bpf, table);
    }
    info!("init io snooper"; "pid" => nix::unistd::getpid().to_string());
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{init_io_snooper, IOContext};
    use std::{fs::OpenOptions, io::Read, io::Write, os::unix::fs::OpenOptionsExt};
    use tempfile::TempDir;

    use libc::O_DIRECT;
    use maligned::A512;
    use maligned::{AsBytes, AsBytesMut};

    #[test]
    fn test_io_context() {
        init_io_snooper().unwrap();

        let tmp = TempDir::new().unwrap();
        let file_path = tmp.path().join("test_io_context");
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();
        let mut w = vec![A512::default(); 2];
        w.as_bytes_mut()[512] = 42;
        let mut ctx = IOContext::new();
        f.write(w.as_bytes()).unwrap();
        f.sync_all().unwrap();
        let delta = ctx.delta_and_refresh();
        assert_ne!(delta.write, 0);
        assert_eq!(delta.read, 0);
        drop(f);

        let mut f = OpenOptions::new()
            .read(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();
        let mut r = vec![A512::default(); 2];
        f.read(&mut r.as_bytes_mut()).unwrap();
        let delta = ctx.delta();
        assert_eq!(delta.write, 0);
        assert_ne!(delta.read, 0);
        drop(f);
    }
}

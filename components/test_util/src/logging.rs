// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    env, fmt,
    fs::{File, OpenOptions},
    io,
    io::prelude::*,
    sync::{Mutex, Once},
};

use slog::{self, Drain, OwnedKVList, Record};

struct Serializer<'a>(&'a mut dyn std::io::Write);

impl<'a> slog::Serializer for Serializer<'a> {
    fn emit_arguments(&mut self, key: slog::Key, val: &std::fmt::Arguments<'_>) -> slog::Result {
        write!(self.0, ", {}: {}", key, val)?;
        Ok(())
    }
}

/// A logger that add a test case tag before each line of log.
struct CaseTraceLogger {
    f: Option<Mutex<File>>,
    skip_tags: Vec<&'static str>,
}

// FIXME: Remove this type when slog::Never implements Display.
#[derive(Debug)]
enum Never {}

impl fmt::Display for Never {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl CaseTraceLogger {
    fn write_log(
        w: &mut dyn std::io::Write,
        record: &Record<'_>,
        values: &OwnedKVList,
        skip_tags: &[&str],
    ) -> Result<(), std::io::Error> {
        use slog::KV;
        if skip_tags.contains(&record.tag()) {
            return Ok(());
        }

        let tag = tikv_util::get_tag_from_thread_name().map_or_else(|| "".to_owned(), |s| s + " ");
        let t = time::now();
        let time_str = time::strftime("%Y/%m/%d %H:%M:%S.%f", &t).unwrap();
        write!(
            w,
            "{}{} {}:{}: [{}] {}",
            tag,
            &time_str[..time_str.len() - 6],
            record.file().rsplit('/').next().unwrap(),
            record.line(),
            record.level(),
            record.msg(),
        )?;
        {
            let mut s = Serializer(w);
            record.kv().serialize(record, &mut s)?;
            values.serialize(record, &mut s)?;
        }
        writeln!(w)?;
        w.flush()?;
        Ok(())
    }
}

impl Drain for CaseTraceLogger {
    type Ok = ();
    type Err = Never;
    fn log(&self, record: &Record<'_>, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        if let Some(ref out) = self.f {
            let mut w = out.lock().unwrap();
            let _ = Self::write_log(&mut *w, record, values, &self.skip_tags);
        } else {
            let mut w = io::stderr();
            let _ = Self::write_log(&mut w, record, values, &self.skip_tags);
        }
        Ok(())
    }
}

impl Drop for CaseTraceLogger {
    fn drop(&mut self) {
        if let Some(ref w) = self.f {
            w.lock().unwrap().flush().unwrap();
        }
    }
}

// A help function to initial logger.
pub fn init_log_for_test() {
    static START: Once = Once::new();
    START.call_once(|| {
        let output = env::var("LOG_FILE").ok();
        let level = tikv_util::logger::get_level_by_string(
            &env::var("LOG_LEVEL").unwrap_or_else(|_| "debug".to_owned()),
        )
        .unwrap();
        let append_instead_truncate = env::var("LOG_APPEND").is_ok();
        let writer = output.map(|f| {
            Mutex::new(
                OpenOptions::new()
                    .create(true)
                    .write(!append_instead_truncate)
                    .truncate(!append_instead_truncate)
                    .append(append_instead_truncate)
                    .open(f)
                    .unwrap(),
            )
        });
        // We don't mind set it multiple times.
        // We hardly ever read rocksdb log in tests.
        let drainer = CaseTraceLogger {
            f: writer,
            skip_tags: vec![
                "rocksdb_log",
                "raftdb_log",
                "rocksdb_log_header",
                "raftdb_log_header",
            ],
        };

        // Default disabled log targets for test.
        let disabled_targets = vec!["tokio_core".to_owned(), "tokio_reactor".to_owned()];

        // CaseTraceLogger relies on test's thread name, however slog_async has
        // its own thread, and the name is "".
        // TODO: Enable the slog_async when the [Custom test frameworks][1] is mature,
        //       and hook the slog_async logger to every test cases.
        //
        // [1]: https://github.com/rust-lang/rfcs/blob/master/text/2318-custom-test-frameworks.md
        tikv_util::logger::init_log(
            drainer,
            level,
            false, // disable async drainer
            true,  // init std log
            disabled_targets,
            0,
        )
        .unwrap();
    });
}

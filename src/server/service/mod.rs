// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

mod debug;
mod diagnostics;
mod kv;

pub use self::debug::Service as DebugService;
pub use self::diagnostics::Service as DiagnosticsService;
pub use self::kv::Service as KvService;

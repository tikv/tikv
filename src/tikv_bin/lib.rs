// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate chrono;
extern crate clap;
extern crate libc;
#[cfg(unix)]
extern crate nix;
#[cfg(unix)]
extern crate signal;
extern crate tikv;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
#[macro_use]
extern crate log;
#[cfg(feature = "mem-profiling")]
extern crate jemallocator;

#[macro_use]
pub mod util;

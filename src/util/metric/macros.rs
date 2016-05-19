// Copyright 2016 PingCAP, Inc.
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

#[macro_export]
macro_rules! metric_count {
    ($key:expr, $count:expr) => {
        if let Some(client) = $crate::util::metric::__client() {
            client.incr($key);
        }
    };
}

#[macro_export]
macro_rules! metric_incr {
    ($key:expr) => {
        if let Some(client) = $crate::util::metric::__client() {
            client.incr($key);
        }
    };
}

#[macro_export]
macro_rules! metric_decr {
    ($key:expr) => {
        if let Some(client) = $crate::util::metric::__client() {
            client.decr($key);
        }
    };
}

#[macro_export]
macro_rules! metric_time {
    ($key:expr, $time:expr) => {
        if let Some(client) = $crate::util::metric::__client() {
            client.time($key, time);
        }
    };
}

#[macro_export]
macro_rules! metric_gauge {
    ($key:expr, $value:expr) => {
        if let Some(client) = $crate::util::metric::__client() {
            client.gauge($key, $value);
        }
    };
}

#[macro_export]
macro_rules! metric_mark {
    ($key:expr) => {
        if let Some(client) = $crate::util::metric::__client() {
            client.mark($key);
        }
    };
}

#[macro_export]
macro_rules! metric_meter {
    ($key:expr, $value:expr) => {
        if let Some(client) = $crate::util::metric::__client() {
            client.meter($key, $value);
        }
    };
}

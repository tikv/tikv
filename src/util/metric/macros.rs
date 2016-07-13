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
        if let Some(client) = $crate::util::metric::client() {
            if let Err(e) = client.count($key, $count) {
                warn!("{}", e);
            }
        }
    };
}

#[macro_export]
macro_rules! metric_incr {
    ($key:expr) => {
        if let Some(client) = $crate::util::metric::client() {
            if let Err(e) = client.incr($key) {
                warn!("{}", e);
            }
        }
    };
}

#[macro_export]
macro_rules! metric_decr {
    ($key:expr) => {
        if let Some(client) = $crate::util::metric::client() {
            if let Err(e) = client.decr($key) {
                warn!("{}", e);
            }
        }
    };
}

#[macro_export]
macro_rules! metric_time {
    ($key:expr, $time:expr) => {
        if let Some(client) = $crate::util::metric::client() {
            if let Err(e) = client.time($key, $crate::util::duration_to_ms($time)) {
                warn!("{}", e);
            }
        }
    };
}

#[macro_export]
macro_rules! metric_gauge {
    ($key:expr, $value:expr) => {
        if let Some(client) = $crate::util::metric::client() {
            if let Err(e) = client.gauge($key, $value) {
                warn!("{}", e);
            }
        }
    };
}

#[macro_export]
macro_rules! metric_mark {
    ($key:expr) => {
        if let Some(client) = $crate::util::metric::client() {
            if let Err(e) = client.mark($key) {
                warn!("{}", e);
            }
        }
    };
}

#[macro_export]
macro_rules! metric_meter {
    ($key:expr, $value:expr) => {
        if let Some(client) = $crate::util::metric::client() {
            if let Err(e) = client.meter($key, $value) {
                warn!("{}", e);
            }
        }
    };
}

// Register a metric
#[macro_export]
macro_rules! register_metric {
    ($(static ref $N:ident : $T:ty = $e:expr; $label:expr)*) => {
        $(
            lazy_static!(static ref $N : $T = $e;);
        )*

        use metrics;
        pub fn __metrics() -> Vec<(&'static str, metrics::metrics::Metric)> {
            let mut temp_vec = Vec::<(&'static str, metrics::metrics::Metric)>::new();
            $(
                match *$N {
                    metrics::metrics::Metric::Counter(ref c) => {
                        temp_vec.push(($label, metrics::metrics::Metric::Counter(c.clone())))
                    }
                    metrics::metrics::Metric::Gauge(ref g) => {
                        temp_vec.push(($label, metrics::metrics::Metric::Gauge(g.clone())))
                    }
                    metrics::metrics::Metric::Meter(ref m) => {
                        temp_vec.push(($label, metrics::metrics::Metric::Meter(m.clone())))
                    }
                    metrics::metrics::Metric::Histogram(ref h) => {
                        temp_vec.push(($label, metrics::metrics::Metric::Histogram(h.clone())))
                    }
                }
            )*
            temp_vec
        }
    };
}

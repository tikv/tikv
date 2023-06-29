// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]
#![feature(assert_matches)]
#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_failpoint_tests)]

#[allow(dead_code)]
#[path = "../integrations/cluster.rs"]
mod cluster;
mod test_basic_write;
mod test_bootstrap;
mod test_bucket;
mod test_life;
mod test_merge;
mod test_pd_heartbeat;
mod test_split;
mod test_trace_apply;

// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]
#![feature(assert_matches)]
#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_tests)]

// TODO: test conflict control in integration tests after split is supported.

mod cluster;
mod test_basic_write;
mod test_conf_change;
mod test_life;
mod test_pd_heartbeat;
mod test_read;
mod test_split;
mod test_status;

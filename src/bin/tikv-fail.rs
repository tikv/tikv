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

#![feature(plugin)]
#![cfg_attr(feature = "dev", plugin(clippy))]
#![cfg_attr(not(feature = "dev"), allow(unknown_lints))]
#![allow(needless_pass_by_value)]

/// Inject failures to TikV.
///
/// TODO: Integrate into tikv-ctl.
///
/// # Examples
///
/// ```sh
/// ./tikv-fail -a 127.0.0.1:9090 inject\
//      -f tikv::raftstore::store::store::raft_between_save\
///     -a "panic(fail_point_raft_between_write)"
/// ```
///

extern crate tikv;
extern crate clap;
extern crate grpcio as grpc;
extern crate protobuf;
extern crate kvproto;

use std::str;
use std::time::Duration;
use std::sync::Arc;

use clap::{App, Arg, SubCommand};
use grpc::{CallOption, ChannelBuilder, EnvBuilder};
use protobuf::RepeatedField;
use kvproto::debugpb;
use kvproto::debugpb_grpc::DebugClient;

fn main() {
    let app = App::new("TiKV fail point")
        .author("PingCAP")
        .about(
            "Distributed transactional key value database powered by Rust and Raft",
        )
        .arg(
            Arg::with_name("addr")
                .short("a")
                .takes_value(true)
                .help("set tikv ip:port"),
        )
        .subcommand(
            SubCommand::with_name("inject")
                .about("Inject failures")
                .arg(
                    Arg::with_name("failpoint")
                        .short("f")
                        .takes_value(true)
                        .help("Fail point name"),
                )
                .arg(
                    Arg::with_name("action")
                        .short("a")
                        .takes_value(true)
                        .help("Actions that injects to the fail point"),
                ),
        )
        .subcommand(
            SubCommand::with_name("recover")
                .about("Recover failures")
                .arg(
                    Arg::with_name("failpoint")
                        .short("f")
                        .takes_value(true)
                        .help("Fail point name"),
                ),
        );
    let matches = app.clone().get_matches();
    let addr = matches.value_of("addr").unwrap();
    let addr = addr.trim_left_matches("http://");

    let env = Arc::new(EnvBuilder::new().name_prefix("tikv-fail").build());
    let channel = ChannelBuilder::new(env).connect(addr);
    let client = DebugClient::new(channel);

    let mut failures = vec![];
    if let Some(matches) = matches.subcommand_matches("inject") {
        let mut failure = debugpb::Failure::new();
        failure.set_field_type(debugpb::Failure_Type::INJECT);
        failure.set_name(matches.value_of("failpoint").unwrap().to_owned());
        failure.set_actions(matches.value_of("action").unwrap().to_owned());
        failures.push(failure);
    } else if let Some(matches) = matches.subcommand_matches("recover") {
        let mut failure = debugpb::Failure::new();
        failure.set_field_type(debugpb::Failure_Type::RECOVER);
        failure.set_name(matches.value_of("failpoint").unwrap().to_owned());
        failures.push(failure);
    }

    let mut req = debugpb::FailPointRequest::new();
    req.set_failures(RepeatedField::from_vec(failures));
    println!("Req {:?}", req);

    let option = CallOption::default().timeout(Duration::from_secs(10));
    client.fail_point_opt(req.clone(), option).unwrap();
    println!("Done!");
}

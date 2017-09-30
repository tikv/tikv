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

/// Inject failures to `TikV`.
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

use std::fs;
use std::io::Read;
use std::str;
use std::time::Duration;
use std::sync::Arc;

use clap::{App, Arg, ArgMatches, SubCommand};
use grpc::{CallOption, ChannelBuilder, EnvBuilder};
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
                    Arg::with_name("actions")
                        .short("a")
                        .takes_value(true)
                        .help("A list of fail point and action to inject"),
                )
                .arg(
                    Arg::with_name("list")
                        .short("l")
                        .takes_value(true)
                        .help("Recover a list of fail points"),
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
                )
                .arg(
                    Arg::with_name("list")
                        .short("l")
                        .takes_value(true)
                        .help("A list of fail point to inject"),
                ),
        );
    let matches = app.clone().get_matches();
    let addr = matches.value_of("addr").unwrap();
    let addr = addr.trim_left_matches("http://");

    let env = Arc::new(EnvBuilder::new().name_prefix("tikv-fail").build());
    let channel = ChannelBuilder::new(env).connect(addr);
    let client = DebugClient::new(channel);

    if let Some(matches) = matches.subcommand_matches("inject") {
        let mut list = read_list(matches);
        if let (Some(name), Some(actions)) =
            (matches.value_of("failpoint"), matches.value_of("actions"))
        {
            list.push((name.to_owned(), actions.to_owned()));
        }

        for (name, actions) in list {
            let mut inject_req = debugpb::InjectFailPointRequest::new();
            inject_req.set_name(name);
            inject_req.set_actions(actions);

            println!("Req {:?}", inject_req);
            let option = CallOption::default().timeout(Duration::from_secs(10));
            client.inject_fail_point_opt(inject_req, option).unwrap();
            println!("Done!");
        }
    } else if let Some(matches) = matches.subcommand_matches("recover") {
        let mut list = read_list(matches);
        if let Some(name) = matches.value_of("failpoint") {
            list.push((name.to_owned(), "".to_owned()));
        }

        for (name, _) in list {
            let mut recover_req = debugpb::RecoverFailPointRequest::new();
            recover_req.set_name(name);

            println!("Req {:?}", recover_req);
            let option = CallOption::default().timeout(Duration::from_secs(10));
            client.recover_fail_point_opt(recover_req, option).unwrap();
            println!("Done!");
        }
    }
}

fn read_list(matches: &ArgMatches) -> Vec<(String, String)> {
    let mut list = vec![];
    if let Some(list_path) = matches.value_of("list") {
        let mut buffer = String::new();
        fs::File::open(list_path)
            .unwrap()
            .read_to_string(&mut buffer)
            .unwrap();
        for line in buffer.lines() {
            let mut parts = line.split('=');
            list.push((
                parts.next().unwrap().to_owned(),
                parts.next().unwrap().to_owned(),
            ))
        }
    }
    list
}

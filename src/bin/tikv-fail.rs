// Copyright 2017 PingCAP, Inc.
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

extern crate clap;
extern crate grpcio as grpc;
extern crate kvproto;
extern crate protobuf;
/// Inject failures to `TikV`.
///
/// TODO: Integrate into tikv-ctl.
///
/// # Examples
///
/// ```sh
/// ./tikv-fail -a 127.0.0.1:9090 inject\
//      raft_between_save=panic("boom")
/// ```
///
extern crate tikv;

use std::fs;
use std::io::{BufRead, BufReader};
use std::str;
use std::sync::Arc;
use std::time::Duration;

use clap::{App, Arg, SubCommand};
use grpc::{CallOption, ChannelBuilder, EnvBuilder};
use kvproto::debugpb;
use kvproto::debugpb_grpc::DebugClient;

fn main() {
    let app = App::new("TiKV fail point")
        .author("PingCAP")
        .about("A tool for injecting failures to TiKV and recovery")
        .arg(
            Arg::with_name("addr")
                .required(true)
                .short("a")
                .takes_value(true)
                .help("set tikv ip:port"),
        )
        .subcommand(
            SubCommand::with_name("inject")
                .about("Inject failures")
                .arg(
                    Arg::with_name("args")
                        .multiple(true)
                        .takes_value(true)
                        .help(
                            "Inject fail point and actions pairs.\
                             E.g. tikv-fail inject fail::a=off fail::b=panic",
                        ),
                )
                .arg(
                    Arg::with_name("file")
                        .short("f")
                        .takes_value(true)
                        .help("Read a file of fail points and actions to inject"),
                ),
        )
        .subcommand(
            SubCommand::with_name("recover")
                .about("Recover failures")
                .arg(
                    Arg::with_name("args")
                        .multiple(true)
                        .takes_value(true)
                        .help("Recover fail points. Eg. tikv-fail recover fail::a fail::b"),
                )
                .arg(
                    Arg::with_name("file")
                        .short("f")
                        .takes_value(true)
                        .help("Recover from a file of fail points"),
                ),
        )
        .subcommand(SubCommand::with_name("list").about("List all fail points"));
    let matches = app.clone().get_matches();
    let addr = matches.value_of("addr").unwrap();
    let addr = addr.trim_left_matches("http://");

    let env = Arc::new(EnvBuilder::new().name_prefix("tikv-fail").build());
    let channel = ChannelBuilder::new(env).connect(addr);
    let client = DebugClient::new(channel);

    if let Some(matches) = matches.subcommand_matches("inject") {
        let mut list = matches.value_of("file").map_or_else(Vec::new, read_file);
        if let Some(ps) = matches.values_of("args") {
            for pair in ps {
                let mut parts = pair.split('=');
                list.push((
                    parts.next().unwrap().to_owned(),
                    parts.next().unwrap_or("").to_owned(),
                ))
            }
        }

        for (name, actions) in list {
            if actions.is_empty() {
                println!("No action for fail point {}", name);
                continue;
            }
            let mut inject_req = debugpb::InjectFailPointRequest::new();
            inject_req.set_name(name);
            inject_req.set_actions(actions);

            let option = CallOption::default().timeout(Duration::from_secs(10));
            client.inject_fail_point_opt(&inject_req, option).unwrap();
        }
    } else if let Some(matches) = matches.subcommand_matches("recover") {
        let mut list = matches.value_of("file").map_or_else(Vec::new, read_file);
        if let Some(fps) = matches.values_of("args") {
            for fp in fps {
                list.push((fp.to_owned(), "".to_owned()))
            }
        }

        for (name, _) in list {
            let mut recover_req = debugpb::RecoverFailPointRequest::new();
            recover_req.set_name(name);

            let option = CallOption::default().timeout(Duration::from_secs(10));
            client.recover_fail_point_opt(&recover_req, option).unwrap();
        }
    } else if matches.is_present("list") {
        let list_req = debugpb::ListFailPointsRequest::new();
        let option = CallOption::default().timeout(Duration::from_secs(10));
        let resp = client.list_fail_points_opt(&list_req, option).unwrap();
        println!("{:?}", resp.get_entries());
    }
}

fn read_file(path: &str) -> Vec<(String, String)> {
    let f = fs::File::open(path).unwrap();
    let f = BufReader::new(f);

    let mut list = vec![];
    for line in f.lines() {
        let line = line.unwrap();
        let mut parts = line.split('=');
        list.push((
            parts.next().unwrap().to_owned(),
            parts.next().unwrap_or("").to_owned(),
        ))
    }
    list
}

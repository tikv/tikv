// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::Duration;

use ::phybr::phybr::RegionMeta;
use ::phybr::phybr_grpc::PhybrClient;
use futures::executor::block_on;
use futures::future::ready;
use futures::sink::SinkExt;
use futures::stream::{self, Stream, StreamExt};
use grpcio::{ChannelBuilder, Environment, Error as GrpcError, WriteFlags};

fn main() {
    let channel = ChannelBuilder::new(Arc::new(Environment::new(1)))
        .keepalive_time(Duration::from_secs(10))
        .keepalive_timeout(Duration::from_secs(3))
        .connect("127.0.0.1:3379");
    let client = PhybrClient::new(channel);
    let (mut sink, receiver) = match client.recover_regions() {
        Ok((x, y)) => (x, y),
        Err(e) => {
            eprintln!("rpc fail: {}", e);
            return;
        }
    };

    let mut region_metas = gen_region_metas();
    let res = block_on(sink.send_all(&mut region_metas));
    println!("send: {:?}", res);
    let res = block_on(sink.close());
    println!("send close: {:?}", res);
    let res = block_on(receiver.for_each(|x| ready(println!("receive: {:?}", x))));
    println!("recv res: {:?}", res);
}

fn gen_region_metas() -> impl Stream<Item = Result<(RegionMeta, WriteFlags), GrpcError>> {
    let mut r1 = RegionMeta::default();
    r1.region_id = 1;
    r1.term = 2;
    r1.applied_index = 100;
    r1.version = 2;
    r1.start_key = b"".to_vec();
    r1.end_key = b"k".to_vec();
    let mut r2 = RegionMeta::default();
    r2.region_id = 2;
    r2.term = 2;
    r2.applied_index = 100;
    r2.version = 2;
    r2.start_key = b"k".to_vec();
    r2.end_key = b"".to_vec();
    stream::iter(vec![
        Ok((r1, WriteFlags::default())),
        Ok((r2, WriteFlags::default())),
    ])
}

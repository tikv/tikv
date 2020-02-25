// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use futures::{Future, Sink, Stream};
use grpcio::*;
use kvproto::tikvpb::BatchCommandsRequest;
use kvproto::tikvpb::TikvClient;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;
use test_raftstore::new_server_cluster;
use tikv_util::HandyRwLock;

#[test]
fn test_batch_commands() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let leader = cluster.get_region(b"").get_peers()[0].clone();
    let addr = cluster.sim.rl().get_addr(leader.get_store_id()).to_owned();

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let (mut sender, receiver) = client.batch_commands().unwrap();
    for _ in 0..1000 {
        let mut batch_req = BatchCommandsRequest::default();
        for i in 0..10 {
            batch_req.mut_requests().push(Default::default());
            batch_req.mut_request_ids().push(i);
        }
        match sender.send((batch_req, WriteFlags::default())).wait() {
            Ok(s) => sender = s,
            Err(e) => panic!("tikv client send fail: {:?}", e),
        }
    }

    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        // We have send 10k requests to the server, so we should get 10k responses.
        let mut count = 0;
        for x in receiver
            .wait()
            .map(move |b| b.unwrap().get_responses().len())
        {
            count += x;
            if count == 10000 {
                tx.send(1).unwrap();
                return;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
}

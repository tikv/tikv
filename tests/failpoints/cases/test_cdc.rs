// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::Cell;
use std::sync::*;
use std::time::Duration;

use futures::{Future, Stream};
use grpcio::{ChannelBuilder, Environment};
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::*;
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::{
    create_change_data, event::Event as Event_oneof_event, ChangeDataClient, ChangeDataRequest,
};

use raft::StateRole;
use raftstore::coprocessor::{CoprocessorHost, ObserverContext, RoleObserver};
use test_raftstore::*;
use tikv_util::worker::Worker;
use tikv_util::HandyRwLock;

use cdc::{CdcObserver, Endpoint};

#[test]
fn test_region_ready_after_deregister() {
    // Prepare the cluster
    // TODO: Code duplicated with components/cdc/tests
    let mut cluster = new_server_cluster(1, 1);

    let id = 1;

    let pd_cli = cluster.pd_client.clone();
    // Create and run cdc endpoints.
    let mut worker = Worker::new(format!("cdc-{}", id));
    let mut sim = cluster.sim.wl();

    // Register cdc service to gRPC server.
    let scheduler = worker.scheduler();
    sim.pending_services
        .entry(id)
        .or_default()
        .push(Box::new(move || {
            create_change_data(cdc::Service::new(scheduler.clone()))
        }));
    let scheduler = worker.scheduler();
    let cdc_ob = CdcObserver::new(scheduler);
    let cdc_ob1 = cdc_ob.clone();
    sim.coprocessor_hooks.entry(id).or_default().push(Box::new(
        move |host: &mut CoprocessorHost| {
            cdc_ob1.register_to(host);
        },
    ));
    // Unlock sim.
    drop(sim);

    cluster.run();

    let raft_router = (*cluster.sim.rl()).get_router(id).unwrap();
    let cdc_endpoint = Endpoint::new(pd_cli, worker.scheduler(), raft_router, cdc_ob.clone());
    worker.start(cdc_endpoint).unwrap();
    let region = cluster.get_region(&[]);
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    let leader_addr = cluster.sim.rl().get_addr(leader.get_store_id()).to_owned();
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&leader_addr);
    let cdc_cli = ChangeDataClient::new(channel);

    let fp = "cdc_incremental_scan_start";
    fail::cfg(fp, "pause").unwrap();

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(region.get_region_epoch().clone());
    let event_feed = cdc_cli.event_feed(&req).unwrap();
    // Sleep for a while to make sure the region has been subscribed
    std::thread::sleep(Duration::from_millis(300));

    // Simulate a role change event
    let mut context = ObserverContext::new(&region);
    cdc_ob.on_role_change(&mut context, StateRole::Follower);

    // Then CDC should not panic
    fail::remove(fp);

    let event_feed_wrap = Cell::new(Some(event_feed));
    let receive_event = |keep_resolved_ts: bool| loop {
        let (change_data, events) =
            match event_feed_wrap.replace(None).unwrap().into_future().wait() {
                Ok(res) => res,
                Err(e) => panic!("receive failed {:?}", e.0),
            };
        event_feed_wrap.set(Some(events));
        let mut change_data = change_data.unwrap();
        assert_eq!(change_data.events.len(), 1);
        let change_data_event = &mut change_data.events[0];
        let event = change_data_event.event.take().unwrap();
        match event {
            Event_oneof_event::ResolvedTs(_) if !keep_resolved_ts => continue,
            other => return other,
        }
    };

    receive_event(false);

    worker.stop().unwrap().join().unwrap();
    cluster.shutdown();
}

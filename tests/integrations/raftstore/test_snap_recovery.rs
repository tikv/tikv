// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use raftstore::store::{PeerMsg, SignificantMsg};
use raft::eraftpb::MessageType;
use test_raftstore::*;
use futures::StreamExt;
use tikv_util::HandyRwLock;
use std::time::Duration;

#[test]
fn test_check_pending_admin() {
	let mut cluster = new_server_cluster(0, 3);
	cluster.pd_client.disable_default_operator();
	cluster.cfg.raft_store.store_io_pool_size = 0;

	cluster.run();

	// write a key to let leader stuck.
	cluster.must_put(b"k", b"v");
	must_get_equal(&cluster.get_engine(1), b"k", b"v");
	must_get_equal(&cluster.get_engine(2), b"k", b"v");
	must_get_equal(&cluster.get_engine(3), b"k", b"v");

	// add filter to make leader cannot commit apply
	cluster.add_send_filter(CloneFilterFactory(
		RegionPacketFilter::new(1, 1)
			.msg_type(MessageType::MsgAppendResponse)
			.direction(Direction::Recv)
		)
	);

	// make a admin request to let leader has pending conf change.
	let leader = new_peer(1, 4);
	cluster.async_add_peer(1, leader).unwrap();

	std::thread::sleep(Duration::from_millis(1000));

	let router = cluster.sim.wl().get_router(1).unwrap();

	let (tx, mut rx) = futures::channel::mpsc::unbounded();
	router.broadcast_normal(|| PeerMsg::SignificantMsg(SignificantMsg::CheckPendingAdmin(tx.clone())));
	futures::executor::block_on(async {
		let r = rx.next().await;
		if let Some(r) = r {
			assert_eq!(r.has_pending_admin, true);
		}
	});

	// clear filter so we can make pending admin requests finished.
	cluster.clear_send_filters();

	std::thread::sleep(Duration::from_millis(1000));

	let (tx, mut rx) = futures::channel::mpsc::unbounded();
	router.broadcast_normal(|| PeerMsg::SignificantMsg(SignificantMsg::CheckPendingAdmin(tx.clone())));
	futures::executor::block_on(async {
		let r = rx.next().await;
		if let Some(r) = r {
			assert_eq!(r.has_pending_admin, false);
		}
	});
}



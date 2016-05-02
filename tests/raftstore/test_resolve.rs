use std::sync::mpsc;

use rand;

use tikv::server::{StoreAddrResolver, PdStoreAddrResolver};
use tikv::pd::PdClient;
use tikv::util::HandyRwLock;

use super::server;
use super::util::*;

#[test]
fn test_resolve() {
    let mut cluster = server::new_server_cluster(0, 5);
    cluster.bootstrap_region().unwrap();

    let pd_client = cluster.pd_client.clone();

    pd_client.wl().put_store(0, new_store(10, "localhost:10".to_owned())).unwrap();
    pd_client.wl().put_store(0, new_store(11, "127.0.0.1:11".to_owned())).unwrap();

    let valid_host = format!("fake_unknown_mock_invalid_{}:12", rand::random::<u64>());
    pd_client.wl().put_store(0, new_store(12, valid_host)).unwrap();

    let r = PdStoreAddrResolver::new(0, pd_client).unwrap();

    let (tx, rx) = mpsc::channel();

    for i in 10..12 {
        let tx1 = tx.clone();
        r.resolve(i,
                  box move |res| {
                      tx1.send(res).unwrap();
                  })
         .unwrap();

        let addr = rx.recv().unwrap().unwrap();
        assert_eq!(format!("{}", addr), format!("127.0.0.1:{}", i));
    }

    let tx1 = tx.clone();
    r.resolve(12,
              box move |res| {
                  tx1.send(res).unwrap();
              })
     .unwrap();

    rx.recv().unwrap().unwrap_err();
}

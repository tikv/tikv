use rand::prelude::*;
use test_raftstore::new_put_cmd;
use test_raftstore_macro::test_test;

#[test_test(test_raftstore::new_node_cluster)]
#[test_test(test_raftstore::new_server_cluster)]
fn test_put() {
    let mut cluster = new_cluster(0, 3);
    cluster.run();

    let mut data_set: Vec<_> = (1..1000)
        .map(|i| {
            (
                format!("key{}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            )
        })
        .collect();

    for kvs in data_set.chunks(50) {
        let requests = kvs.iter().map(|(k, v)| new_put_cmd(k, v)).collect();
        // key9 is always the last region.
        cluster.batch_put(b"key9", requests).unwrap();
    }
    let mut rng = rand::thread_rng();
    for _ in 0..50 {
        let (key, value) = data_set.choose(&mut rng).unwrap();
        let v = cluster.get(key);
        assert_eq!(v.as_ref(), Some(value));
    }

    data_set = data_set
        .into_iter()
        .enumerate()
        .map(|(i, (k, _))| (k, format!("value{}", i + 2).into_bytes()))
        .collect();

    for kvs in data_set.chunks(50) {
        let requests = kvs.iter().map(|(k, v)| new_put_cmd(k, v)).collect();
        // key9 is always the last region.
        cluster.batch_put(b"key9", requests).unwrap();
    }
    // value should be overwrited.
    for _ in 0..50 {
        let (key, value) = data_set.choose(&mut rng).unwrap();
        let v = cluster.get(key);
        assert_eq!(v.as_ref(), Some(value));
    }
}

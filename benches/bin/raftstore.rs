use test::BenchSamples;
use rocksdb::Writable;

use test_util::*;
use util::*;
use cluster::*;
use node::new_node_cluster;
use server::new_server_cluster;

use super::print_result;

const DEFAULT_DATA_SIZE: usize = 100_000;

fn print_set_progress(tag: &str, ncnt: usize, vlen: usize) {
    printf!("benching Set on {},\tnodes: {}, value len: {:4}\t...",
            tag,
            ncnt,
            vlen);
}

fn print_other_progress(tag: &str, action: &str, ncnt: usize) {
    printf!("benching {} on {},\tnodes: {}\t...", action, tag, ncnt);
}

fn bench_set<T: Simulator>(mut cluster: Cluster<T>, value_size: usize) -> BenchSamples {
    prepare_cluster(&mut cluster, &[]);

    let mut kvs = KvGenerator::new(100, value_size);

    bench!{
            let (k, v) = kvs.next().unwrap();
            cluster.put(&k, &v)
    }
}

fn bench_get<T: Simulator>(mut cluster: Cluster<T>) -> BenchSamples {
    let mut kvs = generate_random_kvs(DEFAULT_DATA_SIZE, 100, 128);
    prepare_cluster(&mut cluster, &kvs);

    let mut keys = kvs.drain(..)
                      .take(DEFAULT_DATA_SIZE / 10)
                      .map(|i| i.0)
                      .chain(KvGenerator::new(100, 0).map(|i| i.0));

    bench!{
            let k = keys.next().unwrap();
            cluster.get(&k)
    }
}

fn bench_seek<T: Simulator>(mut cluster: Cluster<T>) -> BenchSamples {
    let mut kvs = generate_random_kvs(DEFAULT_DATA_SIZE, 100, 128);
    prepare_cluster(&mut cluster, &kvs);

    let mut keys = kvs.drain(..)
                      .take(DEFAULT_DATA_SIZE / 10)
                      .map(|i| i.0)
                      .chain(KvGenerator::new(100, 0).map(|i| i.0));

    bench!{
            let k = keys.next().unwrap();
            cluster.seek(&k)
    }
}

fn bench_delete<T: Simulator>(mut cluster: Cluster<T>) -> BenchSamples {
    let mut kvs = generate_random_kvs(DEFAULT_DATA_SIZE, 100, 128);
    prepare_cluster(&mut cluster, &kvs);

    let mut keys = kvs.drain(..)
                      .take(DEFAULT_DATA_SIZE / 10)
                      .map(|i| i.0)
                      .chain(KvGenerator::new(100, 0).map(|i| i.0));

    bench!{
            let k = keys.next().unwrap();
            cluster.delete(&k)
    }
}

fn bench_raft_cluster<T, F>(factory: F, tag: &'static str)
    where T: Simulator,
          F: Fn(u64, usize) -> Cluster<T>
{
    let node_cnts = vec![1, 3, 5];
    let bytes_lens = vec![8, 128, 1024, 4096];

    for ncnt in node_cnts {
        for &vlen in &bytes_lens {
            print_set_progress(tag, ncnt, vlen);
            print_result(bench_set(factory(1, ncnt), vlen));
        }
        print_other_progress(tag, "Get", ncnt);
        print_result(bench_get(factory(1, ncnt)));
        print_other_progress(tag, "Seek", ncnt);
        print_result(bench_seek(factory(1, ncnt)));
        print_other_progress(tag, "Delete", ncnt);
        print_result(bench_delete(factory(1, ncnt)));
    }
}

pub fn bench_raftstore() {
    bench_raft_cluster(new_node_cluster, "raft cluster (channel)");
    bench_raft_cluster(new_server_cluster, "raft cluster (tcp)");
}

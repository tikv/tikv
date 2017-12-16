extern crate rand;

//use std::sync::mpsc::{channel, Sender};
use tikv::storage::{new_local_engine, CFStatistics, Engine, Key, Modify, ScanMode, Snapshot,
                    Value, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_WRITE, TEMP_DIR};
use kvproto::kvrpcpb::Context;
use tikv::raftstore::store::engine::IterOption;
use tempdir::TempDir;

use rand::Rng;
use test::BenchSamples;
use super::print_result;

//use tikv::util::codec::number::NumberEncoder;

//use test_util::{KvGenerator, generate_random_kvs};

// TODO: Bench multithreading

struct BenchDataItem {
    key: Key,
    value: Value,
    ts: Vec<u64>,
}


fn next_ts() -> u64 {
    // TODO: Make this thread safe
    static mut CURRENT_TS: u64 = 0;
    unsafe {
        CURRENT_TS += 100;
        CURRENT_TS
    }
}

fn create_engine() -> Box<Engine> {
    //    let dir = TempDir::new("engine_bench").unwrap();
    let e = new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
    e
}

#[inline]
fn to_bytes(value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    // big endian
    for shift in (0..8).rev() {
        result.push(((value >> shift) & 0xff) as u8);
    }
    result
}

fn create_row_key(tableid: u64, rowid: u64) -> Key {
    let mut result = to_bytes(tableid);
    result.extend(b"_r".iter());
    result.extend(to_bytes(rowid).iter());
    let result = Key::from_raw(&result[..]);
    // result.append_ts(ts)
    result
}

fn generate_row_data(rows: usize, ts_count: usize, value_length: usize) -> Vec<BenchDataItem> {
    let mut rng = rand::thread_rng();
    let mut result = Vec::new();
    for i in 0..rows {
        let mut v = vec![0; value_length];
        rng.fill_bytes(&mut v);
        let key = create_row_key(0, i as u64);
        let item = BenchDataItem {
            key: key.clone(),
            value: v,
            ts: Vec::new(),
        };
        result.push(item);
    }

    for _ in 0..ts_count {
        for i in 0..rows {
            result[i].ts.push(next_ts());
        }
    }

    result
}

fn load_test_data(e: &Box<Engine>, data: &Vec<BenchDataItem>) {
    for ts in 0..data[0].ts.len() {
        for ref item in data {
            let modify_default = Modify::Put(
                CF_DEFAULT,
                item.key.append_ts(item.ts[ts]),
                item.value.clone(),
            );
            // Dummy value in CF_WRITE
            let modify_write = Modify::Put(CF_WRITE, item.key.append_ts(item.ts[ts]), vec![0u8; 9]);
            e.write(&Context::new(), vec![modify_default, modify_write])
                .unwrap();
        }
    }
}


// Simulated load_lock method
#[inline]
fn load_lock(snapshot: &Box<Snapshot>, key: &Key, statistics: &mut CFStatistics) {
    let option = IterOption::new(None, None, false);
    let mut cursor = snapshot.iter_cf(CF_LOCK, option, ScanMode::Mixed).unwrap();
    cursor.get(key, statistics).unwrap();
}

// Simulated seek_write method
#[inline]
fn seek_write(snapshot: &Box<Snapshot>, key: &Key, statistics: &mut CFStatistics) {
    let option = IterOption::default()
        .use_prefix_seek()
        .set_prefix_same_as_start(true);
    let mut cursor = snapshot.iter_cf(CF_WRITE, option, ScanMode::Mixed).unwrap();
    cursor.near_seek(key, statistics).unwrap();
}

// Simulated load_data method
#[inline]
fn load_data(
    snapshot: &Box<Snapshot>,
    key: &Key,
    expected_value: &Option<&[u8]>,
    statistics: &mut CFStatistics,
) {
    // Should fill_cache be true?
    let option = IterOption::new(None, None, false);
    let mut cursor = snapshot.iter(option, ScanMode::Mixed).unwrap();
    let value = cursor.get(key, statistics).unwrap();
    assert_eq!(value, *expected_value);
    //    println!("{:?}", expected_value);
}

#[inline]
fn prewrite(
    e: &Box<Engine>,
    key: &Key,
    value: &Value,
    is_delete: bool,
    ts: u64,
    statistics: &mut CFStatistics,
) {
    let snapshot = e.snapshot(&Context::new()).unwrap();
    seek_write(&snapshot, &key.append_ts(ts), statistics);
    load_lock(&snapshot, &key, statistics);
    // Dummy value in CF_LOCK
    let mut modifies = Vec::new();
    let lock_key = Modify::Put(CF_LOCK, key.clone(), vec![0u8; 25]);
    modifies.push(lock_key);
    if !is_delete {
        // Is it possible to avoid cloning?
        let put_value = Modify::Put(CF_DEFAULT, key.append_ts(ts), value.clone());
        modifies.push(put_value);
    }
    e.write(&Context::new(), modifies).unwrap();
}

#[inline]
fn commit(e: &Box<Engine>, key: &Key, commit_ts: u64, statistics: &mut CFStatistics) {
    let snapshot = e.snapshot(&Context::new()).unwrap();
    load_lock(&snapshot, &key, statistics);
    // Dummy value in CF_WRITE
    let put_write = Modify::Put(CF_WRITE, key.append_ts(commit_ts), vec![0u8; 9]);
    let unlock_key = Modify::Delete(CF_LOCK, key.clone());
    e.write(&Context::new(), vec![put_write, unlock_key])
        .unwrap();
}



//fn cleanup() {}

//fn rollback() {}

//fn resolve_lock() {}


fn bench_get(e: &Box<Engine>, data: &Vec<BenchDataItem>) -> BenchSamples {
    let snapshot = e.snapshot(&Context::new()).unwrap();
    let mut rng = rand::thread_rng();
    let mut fake_statistics = CFStatistics::default();
    bench!{
        let item = &data[rng.gen_range(0, data.len())];
        let ts = item.ts[rng.gen_range(0, item.ts.len())];
        load_lock(&snapshot, &item.key, &mut fake_statistics);
        seek_write(&snapshot, &item.key.append_ts(ts), &mut fake_statistics);
        // Not calculating ts in CF_DEFAULT but simply reuse the ts, for only benching purpose
        load_data(&snapshot, &item.key.append_ts(ts), &Some(&item.value[..]), &mut fake_statistics)
    }
}

// Any difference between update and insert?
fn bench_set(e: &Box<Engine>, data: &Vec<BenchDataItem>) -> BenchSamples {
    //    let snapshot = e.snapshot(&Context::new()).unwrap();
    let mut rng = rand::thread_rng();
    let mut fake_statistics = CFStatistics::default();

    bench!{
        let item = &data[rng.gen_range(0, data.len())];
        prewrite(&e, &item.key, &item.value, false, next_ts(), &mut fake_statistics);
        commit(&e, &item.key, next_ts(), &mut fake_statistics)
    }
}

fn bench_delete(e: &Box<Engine>, data: &Vec<BenchDataItem>) -> BenchSamples {
    //    let snapshot = e.snapshot(&Context::new()).unwrap();
    let mut rng = rand::thread_rng();
    let mut fake_statistics = CFStatistics::default();

    bench!{
        let item = &data[rng.gen_range(0, data.len())];
        prewrite(&e, &item.key, &Vec::new(), true, next_ts(), &mut fake_statistics);
        commit(&e, &item.key, next_ts(), &mut fake_statistics)
    }
}

fn bench_single_gc(e: &Box<Engine>, data: &Vec<BenchDataItem>) -> BenchSamples {
    let snapshot = e.snapshot(&Context::new()).unwrap();
    //    let mut rng = rand::thread_rng();
    let mut fake_statistics = CFStatistics::default();

    let mut it = data.iter();
    bench!{
        // let item = &data[rng.gen_range(0, data.len())];
        let item = it.next().unwrap();
        // Simulating the last write is Put, so the last version is kept after gc
        let ts = u64::max_value();
        seek_write(&snapshot, &item.key.append_ts(ts), &mut fake_statistics);

        let mut modifies = Vec::new();

        for ts in item.ts[0..item.ts.len()-1].iter().rev() {
            let key = item.key.append_ts(*ts);
            seek_write(&snapshot, &key, &mut fake_statistics);
            modifies.push(Modify::Delete(CF_WRITE, key.clone()));
            modifies.push(Modify::Delete(CF_DEFAULT, key));
        };
        e.write(&Context::new(), modifies).unwrap()
    }
}


pub fn bench_engine() {
    for table_size in [1_000, 10_000, 100_000].iter() {
        for version_count in [1, 5, 50].iter() {
            for value_length in [32, 128, 1024].iter() {
                {
                    let e = create_engine();
                    let data = generate_row_data(*table_size, *version_count, *value_length);
                    load_test_data(&e, &data);

                    printf!(
                        "benching txn get on local engine\trows:{} versions:{} value len:{}\t...",
                        table_size,
                        version_count,
                        value_length
                    );
                    print_result(bench_get(&e, &data));

                    printf!(
                        "benching txn set on local engine\trows:{} versions:{} value len:{}\t...",
                        table_size,
                        version_count,
                        value_length
                    );
                    print_result(bench_set(&e, &data));
                } // Dispose e and data, regenerate new ones

                {

                    let e = create_engine();
                    let data = generate_row_data(*table_size, *version_count, *value_length);

                    load_test_data(&e, &data);

                    printf!(
                        "benching txn del on local engine\trows:{} versions:{} value len:{}\t...",
                        *table_size,
                        version_count,
                        value_length
                    );
                    print_result(bench_delete(&e, &data));
                }
            }
        }
    }


    for version_count in [1, 5, 50].iter() {
        for value_length in [32, 128, 1024].iter() {
            let e = create_engine();
            let data = generate_row_data(100_000, *version_count, *value_length);

            load_test_data(&e, &data);

            printf!(
                "benching gc on local engine\trows:{} versions:{} value len:{}\t...",
                100_000,
                version_count,
                value_length
            );
            print_result(bench_single_gc(&e, &data));
        }
    }

}

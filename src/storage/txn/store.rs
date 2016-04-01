use kvproto::kvrpcpb::Context;
use storage::{Key, Value, KvPair, Mutation};
use storage::Engine;
use storage::mvcc::{MvccTxn, Error as MvccError};
use super::shard_mutex::ShardMutex;
use super::{Error, Result};

pub struct TxnStore {
    engine: Box<Engine>,
    shard_mutex: ShardMutex,
}

const SHARD_MUTEX_SIZE: usize = 256;

impl TxnStore {
    pub fn new(engine: Box<Engine>) -> TxnStore {
        TxnStore {
            engine: engine,
            shard_mutex: ShardMutex::new(SHARD_MUTEX_SIZE),
        }
    }

    pub fn get(&self, ctx: Context, key: &Key, start_ts: u64) -> Result<Option<Value>> {
        let _guard = self.shard_mutex.lock(&[key]);
        let txn = MvccTxn::new(self.engine.as_ref(), &ctx, start_ts);
        Ok(try!(txn.get(key)))
    }

    #[allow(dead_code)]
    pub fn batch_get(&self,
                     ctx: Context,
                     keys: &[Key],
                     start_ts: u64)
                     -> Vec<Result<Option<Value>>> {
        let txn = MvccTxn::new(self.engine.as_ref(), &ctx, start_ts);
        let mut results = Vec::<_>::with_capacity(keys.len());
        for k in keys {
            let _guard = self.shard_mutex.lock(&[k]);
            results.push(txn.get(k).map_err(Error::from));
        }
        results
    }

    pub fn scan(&self,
                ctx: Context,
                key: Key,
                limit: usize,
                start_ts: u64)
                -> Result<Vec<Result<KvPair>>> {
        let mut results = vec![];
        let mut key = key;
        let txn = MvccTxn::new(self.engine.as_ref(), &ctx, start_ts);
        while results.len() < limit {
            let next_key = match try!(self.engine.seek(&ctx, &key)) {
                Some((key, _)) => key,
                None => break,
            };
            let _guard = self.shard_mutex.lock(&next_key);
            key = Key::from_raw(next_key.clone());
            match txn.get(&key) {
                Ok(Some(value)) => results.push(Ok((next_key, value))),
                Ok(None) => {}
                e @ Err(MvccError::KeyIsLocked{..}) => {
                    results.push(Err(Error::from(e.unwrap_err())))
                }
                Err(e) => return Err(e.into()),
            };
            key = key.encode_ts(u64::max_value());
        }
        Ok(results)
    }

    pub fn prewrite(&self,
                    ctx: Context,
                    mutations: Vec<Mutation>,
                    primary: Vec<u8>,
                    start_ts: u64)
                    -> Result<Vec<Result<()>>> {
        let mut results = vec![];
        let _gurad = {
            let locked_keys: Vec<&Key> = mutations.iter().map(|x| x.key()).collect();
            self.shard_mutex.lock(&locked_keys)
        };
        let mut txn = MvccTxn::new(self.engine.as_ref(), &ctx, start_ts);
        for m in mutations {
            match txn.prewrite(m, &primary) {
                Ok(_) => results.push(Ok(())),
                e @ Err(MvccError::KeyIsLocked{..}) => results.push(e.map_err(Error::from)),
                Err(e) => return Err(Error::from(e)),
            }
        }
        try!(txn.submit());
        Ok(results)
    }

    pub fn commit(&self,
                  ctx: Context,
                  keys: Vec<Key>,
                  start_ts: u64,
                  commit_ts: u64)
                  -> Result<()> {
        let _guard = self.shard_mutex.lock(&keys);
        let mut txn = MvccTxn::new(self.engine.as_ref(), &ctx, start_ts);
        for k in keys {
            try!(txn.commit(&k, commit_ts));
        }
        try!(txn.submit());
        Ok(())
    }

    pub fn commit_then_get(&self,
                           ctx: Context,
                           key: Key,
                           lock_ts: u64,
                           commit_ts: u64,
                           get_ts: u64)
                           -> Result<Option<Value>> {
        let _guard = self.shard_mutex.lock(&[&key]);
        let mut txn = MvccTxn::new(self.engine.as_ref(), &ctx, lock_ts);
        let val = try!(txn.commit_then_get(&key, commit_ts, get_ts));
        try!(txn.submit());
        Ok(val)
    }

    pub fn cleanup(&self, ctx: Context, key: Key, start_ts: u64) -> Result<()> {
        let _guard = self.shard_mutex.lock(&[&key]);
        let mut txn = MvccTxn::new(self.engine.as_ref(), &ctx, start_ts);
        try!(txn.rollback(&key));
        try!(txn.submit());
        Ok(())
    }

    pub fn rollback(&self, ctx: Context, keys: Vec<Key>, start_ts: u64) -> Result<()> {
        let _guard = self.shard_mutex.lock(&keys);
        let mut txn = MvccTxn::new(self.engine.as_ref(), &ctx, start_ts);
        for k in keys {
            try!(txn.rollback(&k));
        }
        try!(txn.submit());
        Ok(())
    }

    #[allow(dead_code)]
    pub fn rollback_then_get(&self, ctx: Context, key: Key, lock_ts: u64) -> Result<Option<Value>> {
        let _guard = self.shard_mutex.lock(&[&key]);
        let mut txn = MvccTxn::new(self.engine.as_ref(), &ctx, lock_ts);
        let val = try!(txn.rollback_then_get(&key));
        try!(txn.submit());
        Ok(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvproto::kvrpcpb::Context;
    use storage::{Mutation, Key, KvPair, make_key};
    use storage::engine::{self, Dsn};
    use util::codec::bytes;

    trait TxnStoreAssert {
        fn get_none(&self, key: &[u8], ts: u64);
        fn get_err(&self, key: &[u8], ts: u64);
        fn get_ok(&self, key: &[u8], ts: u64, expect: &[u8]);
        fn put_ok(&self, key: &[u8], value: &[u8], start_ts: u64, commit_ts: u64);
        fn delete_ok(&self, key: &[u8], start_ts: u64, commit_ts: u64);
        fn scan_ok(&self,
                   start_key: &[u8],
                   limit: usize,
                   ts: u64,
                   expect: Vec<Option<(&[u8], &[u8])>>);
        fn prewrite_ok(&self, mutations: Vec<Mutation>, primary: &[u8], start_ts: u64);
        fn prewrite_err(&self, mutations: Vec<Mutation>, primary: &[u8], start_ts: u64);
        fn commit_ok(&self, keys: Vec<&[u8]>, start_ts: u64, commit_ts: u64);
        fn commit_err(&self, keys: Vec<&[u8]>, start_ts: u64, commit_ts: u64);
        fn rollback_ok(&self, keys: Vec<&[u8]>, start_ts: u64);
        fn rollback_err(&self, keys: Vec<&[u8]>, start_ts: u64);
        fn commit_then_get_ok(&self,
                              key: &[u8],
                              lock_ts: u64,
                              commit_ts: u64,
                              get_ts: u64,
                              expect: &[u8]);
        fn rollback_then_get_ok(&self, key: &[u8], lock_ts: u64, expect: &[u8]);
    }

    impl TxnStoreAssert for TxnStore {
        fn get_none(&self, key: &[u8], ts: u64) {
            let key = make_key(key);
            assert_eq!(self.get(Context::new(), &key, ts).unwrap(), None);
        }

        fn get_err(&self, key: &[u8], ts: u64) {
            let key = make_key(key);
            assert!(self.get(Context::new(), &key, ts).is_err());
        }

        fn get_ok(&self, key: &[u8], ts: u64, expect: &[u8]) {
            let key = make_key(key);
            assert_eq!(self.get(Context::new(), &key, ts).unwrap().unwrap(), expect);
        }

        fn put_ok(&self, key: &[u8], value: &[u8], start_ts: u64, commit_ts: u64) {
            self.prewrite(Context::new(),
                          vec![Mutation::Put((make_key(key), value.to_vec()))],
                          key.to_vec(),
                          start_ts)
                .unwrap();
            self.commit(Context::new(), vec![make_key(key)], start_ts, commit_ts).unwrap();
        }

        fn delete_ok(&self, key: &[u8], start_ts: u64, commit_ts: u64) {
            self.prewrite(Context::new(),
                          vec![Mutation::Delete(make_key(key))],
                          key.to_vec(),
                          start_ts)
                .unwrap();
            self.commit(Context::new(), vec![make_key(key)], start_ts, commit_ts).unwrap();
        }

        fn scan_ok(&self,
                   start_key: &[u8],
                   limit: usize,
                   ts: u64,
                   expect: Vec<Option<(&[u8], &[u8])>>) {
            let key_address = make_key(start_key);
            let result = self.scan(Context::new(), key_address, limit, ts).unwrap();
            let result: Vec<Option<KvPair>> = result.into_iter()
                                                    .map(Result::ok)
                                                    .collect();
            let expect: Vec<Option<KvPair>> = expect.into_iter()
                                                    .map(|x| {
                                                        x.map(|(k, v)| {
                                                            (bytes::encode_bytes(k), v.to_vec())
                                                        })
                                                    })
                                                    .collect();
            assert_eq!(result, expect);
        }

        fn prewrite_ok(&self, mutations: Vec<Mutation>, primary: &[u8], start_ts: u64) {
            self.prewrite(Context::new(), mutations, primary.to_vec(), start_ts).unwrap();
        }

        fn prewrite_err(&self, mutations: Vec<Mutation>, primary: &[u8], start_ts: u64) {
            assert!(self.prewrite(Context::new(), mutations, primary.to_vec(), start_ts)
                        .is_err());
        }

        fn commit_ok(&self, keys: Vec<&[u8]>, start_ts: u64, commit_ts: u64) {
            let keys: Vec<Key> = keys.iter().map(|x| make_key(x)).collect();
            self.commit(Context::new(), keys, start_ts, commit_ts).unwrap();
        }

        fn commit_err(&self, keys: Vec<&[u8]>, start_ts: u64, commit_ts: u64) {
            let keys: Vec<Key> = keys.iter().map(|x| make_key(x)).collect();
            assert!(self.commit(Context::new(), keys, start_ts, commit_ts).is_err());
        }

        fn rollback_ok(&self, keys: Vec<&[u8]>, start_ts: u64) {
            let keys: Vec<Key> = keys.iter().map(|x| make_key(x)).collect();
            self.rollback(Context::new(), keys, start_ts).unwrap();
        }

        fn rollback_err(&self, keys: Vec<&[u8]>, start_ts: u64) {
            let keys: Vec<Key> = keys.iter().map(|x| make_key(x)).collect();
            assert!(self.rollback(Context::new(), keys, start_ts).is_err());
        }

        fn commit_then_get_ok(&self,
                              key: &[u8],
                              lock_ts: u64,
                              commit_ts: u64,
                              get_ts: u64,
                              expect: &[u8]) {
            assert_eq!(self.commit_then_get(Context::new(),
                                            make_key(key),
                                            lock_ts,
                                            commit_ts,
                                            get_ts)
                           .unwrap()
                           .unwrap(),
                       expect);
        }

        fn rollback_then_get_ok(&self, key: &[u8], lock_ts: u64, expect: &[u8]) {
            assert_eq!(self.rollback_then_get(Context::new(), make_key(key), lock_ts)
                           .unwrap()
                           .unwrap(),
                       expect);
        }
    }

    #[test]
    fn test_txn_store_get() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = TxnStore::new(engine);

        // not exist
        store.get_none(b"x", 10);
        // after put
        store.put_ok(b"x", b"x", 5, 10);
        store.get_none(b"x", 9);
        store.get_ok(b"x", 10, b"x");
        store.get_ok(b"x", 11, b"x");
    }

    #[test]
    fn test_txn_store_delete() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = TxnStore::new(engine);

        store.put_ok(b"x", b"x5-10", 5, 10);
        store.delete_ok(b"x", 15, 20);
        store.get_none(b"x", 5);
        store.get_none(b"x", 9);
        store.get_ok(b"x", 10, b"x5-10");
        store.get_ok(b"x", 19, b"x5-10");
        store.get_none(b"x", 20);
        store.get_none(b"x", 21);
    }

    #[test]
    fn test_txn_store_cleanup_rollback() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = TxnStore::new(engine);

        store.put_ok(b"secondary", b"s-0", 0, 1);
        store.prewrite_ok(vec![Mutation::Put((make_key(b"primary"), b"p-5".to_vec())),
                               Mutation::Put((make_key(b"secondary"), b"s-5".to_vec()))],
                          b"primary",
                          5);
        store.get_err(b"secondary", 10);
        store.rollback_ok(vec![b"primary"], 5);
        store.rollback_then_get_ok(b"secondary", 5, b"s-0");
        store.rollback_then_get_ok(b"secondary", 5, b"s-0");
    }

    #[test]
    fn test_txn_store_cleanup_commit() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = TxnStore::new(engine);

        store.put_ok(b"secondary", b"s-0", 0, 1);
        store.prewrite_ok(vec![Mutation::Put((make_key(b"primary"), b"p-5".to_vec())),
                               Mutation::Put((make_key(b"secondary"), b"s-5".to_vec()))],
                          b"primary",
                          5);
        store.get_err(b"secondary", 8);
        store.get_err(b"secondary", 12);
        store.commit_ok(vec![b"primary"], 5, 10);
        store.rollback_err(vec![b"primary"], 5);
        store.commit_then_get_ok(b"secondary", 5, 10, 8, b"s-0");
        store.commit_then_get_ok(b"secondary", 5, 10, 12, b"s-5");
        store.commit_then_get_ok(b"secondary", 5, 10, 8, b"s-0");
        store.commit_then_get_ok(b"secondary", 5, 10, 12, b"s-5");
    }

    #[test]
    fn test_txn_store_scan() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = TxnStore::new(engine);

        // ver10: A(10) - B(_) - C(10) - D(_) - E(10)
        store.put_ok(b"A", b"A10", 5, 10);
        store.put_ok(b"C", b"C10", 5, 10);
        store.put_ok(b"E", b"E10", 5, 10);

        let check_v10 = || {
            store.scan_ok(b"", 0, 10, vec![]);
            store.scan_ok(b"", 1, 10, vec![Some((b"A", b"A10"))]);
            store.scan_ok(b"", 2, 10, vec![Some((b"A", b"A10")), Some((b"C", b"C10"))]);
            store.scan_ok(b"",
                          3,
                          10,
                          vec![Some((b"A", b"A10")), Some((b"C", b"C10")), Some((b"E", b"E10"))]);
            store.scan_ok(b"",
                          4,
                          10,
                          vec![Some((b"A", b"A10")), Some((b"C", b"C10")), Some((b"E", b"E10"))]);
            store.scan_ok(b"A",
                          3,
                          10,
                          vec![Some((b"A", b"A10")), Some((b"C", b"C10")), Some((b"E", b"E10"))]);
            store.scan_ok(b"A\x00",
                          3,
                          10,
                          vec![Some((b"C", b"C10")), Some((b"E", b"E10"))]);

            store.scan_ok(b"C",
                          4,
                          10,
                          vec![Some((b"C", b"C10")), Some((b"E", b"E10"))]);
            store.scan_ok(b"F", 1, 10, vec![]);

        };
        check_v10();

        // ver20: A(10) - B(20) - C(10) - D(20) - E(10)
        store.put_ok(b"B", b"B20", 15, 20);
        store.put_ok(b"D", b"D20", 15, 20);

        let check_v20 = || {
            store.scan_ok(b"",
                          5,
                          20,
                          vec![Some((b"A", b"A10")),
                               Some((b"B", b"B20")),
                               Some((b"C", b"C10")),
                               Some((b"D", b"D20")),
                               Some((b"E", b"E10"))]);
            store.scan_ok(b"C",
                          5,
                          20,
                          vec![Some((b"C", b"C10")), Some((b"D", b"D20")), Some((b"E", b"E10"))]);
            store.scan_ok(b"D\x00", 1, 20, vec![Some((b"E", b"E10"))]);
        };
        check_v10();
        check_v20();

        // ver30: A(_) - B(20) - C(10) - D(_) - E(10)
        store.delete_ok(b"A", 25, 30);
        store.delete_ok(b"D", 25, 30);

        let check_v30 = || {
            store.scan_ok(b"",
                          5,
                          30,
                          vec![Some((b"B", b"B20")), Some((b"C", b"C10")), Some((b"E", b"E10"))]);
            store.scan_ok(b"A", 1, 30, vec![Some((b"B", b"B20"))]);
            store.scan_ok(b"C\x00", 5, 30, vec![Some((b"E", b"E10"))]);
        };
        check_v10();
        check_v20();
        check_v30();

        // ver40: A(_) - B(_) - C(40) - D(40) - E(10)
        store.delete_ok(b"B", 35, 40);
        store.put_ok(b"C", b"C40", 35, 40);
        store.put_ok(b"D", b"D40", 35, 40);

        let check_v40 = || {
            store.scan_ok(b"",
                          5,
                          40,
                          vec![Some((b"C", b"C40")), Some((b"D", b"D40")), Some((b"E", b"E10"))]);
            store.scan_ok(b"",
                          5,
                          100,
                          vec![Some((b"C", b"C40")), Some((b"D", b"D40")), Some((b"E", b"E10"))]);
        };
        check_v10();
        check_v20();
        check_v30();
        check_v40();
    }

    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use std::thread;
    use rand::random;

    struct Oracle {
        ts: Mutex<u64>,
    }

    impl Oracle {
        fn new() -> Oracle {
            Oracle { ts: Mutex::new(0) }
        }

        fn get_ts(&self) -> u64 {
            let mut ts = self.ts.lock().unwrap();
            *ts += 1;
            *ts
        }
    }

    const INC_MAX_RETRY: usize = 100;

    fn inc(store: &TxnStore, oracle: &Oracle, key: &[u8]) -> Result<i32, ()> {
        let key_address = make_key(key);
        for i in 0..INC_MAX_RETRY {
            let start_ts = oracle.get_ts();
            let number: i32 = match store.get(Context::new(), &key_address, start_ts) {
                Ok(Some(x)) => String::from_utf8(x).unwrap().parse().unwrap(),
                Ok(None) => 0,
                Err(_) => {
                    backoff(i);
                    continue;
                }
            };
            let next = number + 1;
            if let Err(_) = store.prewrite(Context::new(),
                                           vec![Mutation::Put((make_key(key),
                                                               next.to_string().into_bytes()))],
                                           key.to_vec(),
                                           start_ts) {
                backoff(i);
                continue;
            }
            let commit_ts = oracle.get_ts();
            if let Err(_) = store.commit(Context::new(),
                                         vec![key_address.clone()],
                                         start_ts,
                                         commit_ts) {
                backoff(i);
                continue;
            }
            return Ok(number);
        }
        Err(())
    }

    #[test]
    fn test_isolation_inc() {
        const THREAD_NUM: usize = 4;
        const INC_PER_THREAD: usize = 100;

        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = Arc::new(TxnStore::new(engine));
        let oracle = Arc::new(Oracle::new());
        let punch_card = Arc::new(Mutex::new(vec![false; THREAD_NUM * INC_PER_THREAD]));

        let mut threads = vec![];
        for _ in 0..THREAD_NUM {
            let (punch_card, store, oracle) = (punch_card.clone(), store.clone(), oracle.clone());
            threads.push(thread::spawn(move || {
                for _ in 0..INC_PER_THREAD {
                    let number = inc(&store, &oracle, b"key").unwrap() as usize;
                    let mut punch = punch_card.lock().unwrap();
                    assert_eq!(punch[number], false);
                    punch[number] = true;
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(inc(&store, &oracle, b"key").unwrap() as usize,
                   THREAD_NUM * INC_PER_THREAD);
    }

    fn format_key(x: usize) -> Vec<u8> {
        format!("k{}", x).into_bytes()
    }

    fn inc_multi(store: &TxnStore, oracle: &Oracle, n: usize) -> bool {
        'retry: for i in 0..INC_MAX_RETRY {
            let start_ts = oracle.get_ts();
            let keys: Vec<Key> = (0..n).map(format_key).map(|x| make_key(&x)).collect();
            let mut mutations = vec![];
            for key in keys.iter().take(n) {
                let number = match store.get(Context::new(), &key, start_ts) {
                    Ok(Some(n)) => String::from_utf8(n).unwrap().parse().unwrap(),
                    Ok(None) => 0,
                    Err(_) => {
                        backoff(i);
                        continue 'retry;
                    }
                };
                let next = number + 1;
                mutations.push(Mutation::Put((key.clone(), next.to_string().into_bytes())));
            }
            if let Err(_) = store.prewrite(Context::new(), mutations, b"k0".to_vec(), start_ts) {
                backoff(i);
                continue;
            }
            let commit_ts = oracle.get_ts();
            if let Err(_) = store.commit(Context::new(), keys, start_ts, commit_ts) {
                backoff(i);
                continue;
            }
            return true;
        }
        false
    }

    const BACK_OFF_CAP: u64 = 100;

    // Implements exponential backoff with full jitter.
    // See: http://www.awsarchitectureblog.com/2015/03/backoff.html.
    fn backoff(attempts: usize) {
        let upper_ms: u64 = match attempts {
            0...6 => 2u64.pow(attempts as u32),
            _ => BACK_OFF_CAP,
        };
        thread::sleep(Duration::from_millis(random::<u64>() % upper_ms))
    }

    #[test]
    fn test_isolation_multi_inc() {
        const THREAD_NUM: usize = 4;
        const KEY_NUM: usize = 4;
        const INC_PER_THREAD: usize = 100;

        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = Arc::new(TxnStore::new(engine));
        let oracle = Arc::new(Oracle::new());

        let mut threads = vec![];
        for _ in 0..THREAD_NUM {
            let (store, oracle) = (store.clone(), oracle.clone());
            threads.push(thread::spawn(move || {
                for _ in 0..INC_PER_THREAD {
                    assert!(inc_multi(&store, &oracle, KEY_NUM));
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        for n in 0..KEY_NUM {
            assert_eq!(inc(&store, &oracle, &format_key(n)).unwrap() as usize,
                       THREAD_NUM * INC_PER_THREAD);
        }
    }

    use test::Bencher;
    use tempdir::TempDir;

    #[bench]
    fn bench_txn_store_memory_inc(b: &mut Bencher) {
        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = TxnStore::new(engine);
        let oracle = Oracle::new();

        b.iter(|| {
            inc(&store, &oracle, b"key").unwrap();
        });
    }

    #[bench]
    fn bench_txn_store_memory_inc_x100(b: &mut Bencher) {
        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = TxnStore::new(engine);
        let oracle = Oracle::new();

        b.iter(|| {
            inc_multi(&store, &oracle, 100);
        });
    }

    #[bench]
    fn bench_txn_store_rocksdb_inc(b: &mut Bencher) {
        let dir = TempDir::new("rocksdb_test").unwrap();
        let engine = engine::new_engine(Dsn::RocksDBPath(dir.path().to_str().unwrap())).unwrap();
        let store = TxnStore::new(engine);
        let oracle = Oracle::new();

        b.iter(|| {
            inc(&store, &oracle, b"key").unwrap();
        });
    }

    #[bench]
    fn bench_txn_store_rocksdb_inc_x100(b: &mut Bencher) {
        let dir = TempDir::new("rocksdb_test").unwrap();
        let engine = engine::new_engine(Dsn::RocksDBPath(dir.path().to_str().unwrap())).unwrap();
        let store = TxnStore::new(engine);
        let oracle = Oracle::new();

        b.iter(|| {
            inc_multi(&store, &oracle, 100);
        });
    }
}

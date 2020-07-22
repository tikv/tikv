use std::marker::PhantomData;
use std::sync::Arc;

use engine_traits::{KvEngine, Snapshot, CF_RAFT};
use kvproto::metapb::Region;

use crate::Result;

pub struct ConsistencyCheckHost<E: KvEngine> {
    _engine: PhantomData<E>,
    checker: Arc<dyn ConsistencyChecker<Snap = E::Snapshot>>,
}

impl<E: KvEngine> Clone for ConsistencyCheckHost<E> {
    fn clone(&self) -> ConsistencyCheckHost<E> {
        ConsistencyCheckHost {
            _engine: PhantomData::default(),
            checker: self.checker.clone(),
        }
    }
}

impl<E: KvEngine> ConsistencyCheckHost<E> {
    pub fn new() -> ConsistencyCheckHost<E> {
        ConsistencyCheckHost {
            _engine: PhantomData::default(),
            checker: Arc::new(RawConsistencyChecker::<E>::default()),
        }
    }

    pub fn set_mvcc_consistency_checker<C>(&mut self, checker: C)
    where
        C: ConsistencyChecker<Snap = E::Snapshot> + 'static,
    {
        self.checker = Arc::new(checker) as Arc<dyn ConsistencyChecker<Snap = E::Snapshot>>;
    }

    pub fn gen_safe_point(&self) -> u64 {
        self.checker.gen_safe_point()
    }

    pub fn compute_hash(&self, region: &Region, safe_point: u64, snap: E::Snapshot) -> Result<u32> {
        self.checker.compute_hash(region, safe_point, snap)
    }
}

pub trait ConsistencyChecker: Sync + Send {
    type Snap: engine_traits::Snapshot;
    fn gen_safe_point(&self) -> u64;
    fn compute_hash(&self, region: &Region, safe_point: u64, snap: Self::Snap) -> Result<u32>;
}

pub struct RawConsistencyChecker<E: KvEngine>(PhantomData<E>);

impl<E: KvEngine> Default for RawConsistencyChecker<E> {
    fn default() -> RawConsistencyChecker<E> {
        RawConsistencyChecker(Default::default())
    }
}

impl<E: KvEngine> ConsistencyChecker for RawConsistencyChecker<E> {
    type Snap = E::Snapshot;

    fn gen_safe_point(&self) -> u64 {
        0
    }

    fn compute_hash(
        &self,
        region: &kvproto::metapb::Region,
        _safe_point: u64,
        snap: Self::Snap,
    ) -> Result<u32> {
        compute_hash_on_raw(region, snap)
    }
}

fn compute_hash_on_raw<S: Snapshot>(region: &Region, snap: S) -> Result<u32> {
    let region_id = region.get_id();
    let mut digest = crc32fast::Hasher::new();
    let mut cf_names = snap.cf_names();
    cf_names.sort();

    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    for cf in cf_names {
        snap.scan_cf(cf, &start_key, &end_key, false, |k, v| {
            digest.update(k);
            digest.update(v);
            Ok(true)
        })?;
    }

    // Computes the hash from the Region state too.
    let region_state_key = keys::region_state_key(region_id);
    digest.update(&region_state_key);
    match snap.get_value_cf(CF_RAFT, &region_state_key) {
        Err(e) => return Err(e.into()),
        Ok(Some(v)) => digest.update(&v),
        Ok(None) => {}
    }
    Ok(digest.finalize())
}

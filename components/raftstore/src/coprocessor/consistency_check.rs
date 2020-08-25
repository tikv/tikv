use std::marker::PhantomData;

use engine_traits::{KvEngine, Snapshot, CF_RAFT};
use kvproto::metapb::Region;

use crate::coprocessor::{ConsistencyCheckMethod, Coprocessor};
use crate::Result;

pub trait ConsistencyCheckObserver<E: KvEngine>: Coprocessor {
    /// Update context. Return `true` if later observers should be skiped.
    fn update_context(&self, context: &mut Vec<u8>) -> bool;

    /// Compute hash for `region`. Return `0` if the observer is skiped.
    fn compute_hash(&self, region: &Region, context: &mut &[u8], snap: &E::Snapshot)
        -> Result<u32>;
}

#[derive(Clone)]
pub struct RawConsistencyCheckObserver<E: KvEngine>(PhantomData<E>);

impl<E: KvEngine> Coprocessor for RawConsistencyCheckObserver<E> {}

impl<E: KvEngine> Default for RawConsistencyCheckObserver<E> {
    fn default() -> RawConsistencyCheckObserver<E> {
        RawConsistencyCheckObserver(Default::default())
    }
}

impl<E: KvEngine> ConsistencyCheckObserver<E> for RawConsistencyCheckObserver<E> {
    fn update_context(&self, context: &mut Vec<u8>) -> bool {
        context.push(ConsistencyCheckMethod::Raw as u8);
        // Raw consistency check is the most heavy and strong one.
        // So all others can be skiped.
        true
    }

    fn compute_hash(
        &self,
        region: &kvproto::metapb::Region,
        context: &mut &[u8],
        snap: &E::Snapshot,
    ) -> Result<u32> {
        assert!(!context.is_empty());
        assert_eq!(context[0], ConsistencyCheckMethod::Raw as u8);
        *context = &context[1..];
        compute_hash_on_raw(region, snap)
    }
}

fn compute_hash_on_raw<S: Snapshot>(region: &Region, snap: &S) -> Result<u32> {
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

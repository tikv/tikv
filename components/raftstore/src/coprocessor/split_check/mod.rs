// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

mod half;
mod keys;
mod size;
mod table;

use kvproto::{metapb::Region, pdpb::CheckPolicy};
use tikv_util::box_try;

pub use self::{
    half::{get_region_approximate_middle, HalfCheckObserver},
    keys::{get_region_approximate_keys, KeysCheckObserver},
    size::{get_region_approximate_size, SizeCheckObserver},
    table::TableCheckObserver,
};
use super::{config::Config, error::Result, Bucket, KeyEntry, ObserverContext, SplitChecker};

pub struct Host<'a, E> {
    checkers: Vec<Box<dyn SplitChecker<E>>>,
    auto_split: bool,
    cfg: &'a Config,
}

impl<'a, E> Host<'a, E> {
    pub fn new(auto_split: bool, cfg: &'a Config) -> Host<'a, E> {
        Host {
            auto_split,
            checkers: vec![],
            cfg,
        }
    }

    #[inline]
    pub fn auto_split(&self) -> bool {
        self.auto_split
    }

    #[inline]
    pub fn skip(&self) -> bool {
        self.checkers.is_empty()
    }

    pub fn policy(&self) -> CheckPolicy {
        for checker in &self.checkers {
            if checker.policy() == CheckPolicy::Approximate {
                return CheckPolicy::Approximate;
            }
        }
        CheckPolicy::Scan
    }

    /// Hook to call for every check during split.
    ///
    /// Return true means abort early.
    pub fn on_kv(&mut self, region: &Region, entry: &KeyEntry) -> bool {
        let mut ob_ctx = ObserverContext::new(region);
        for checker in &mut self.checkers {
            if checker.on_kv(&mut ob_ctx, entry) {
                return true;
            }
        }
        false
    }

    pub fn split_keys(&mut self) -> Vec<Vec<u8>> {
        for checker in &mut self.checkers {
            let keys = checker.split_keys();
            if !keys.is_empty() {
                return keys;
            }
        }
        vec![]
    }

    pub fn approximate_split_keys(&mut self, region: &Region, engine: &E) -> Result<Vec<Vec<u8>>> {
        for checker in &mut self.checkers {
            let keys = box_try!(checker.approximate_split_keys(region, engine));
            if !keys.is_empty() {
                return Ok(keys);
            }
        }
        Ok(vec![])
    }

    pub fn approximate_bucket_keys<Kv: engine_traits::KvEngine>(
        &mut self,
        region: &Region,
        engine: &Kv,
    ) -> Result<Bucket> {
        let region_size = get_region_approximate_size(engine, region, 0)?;
        const MIN_BUCKET_COUNT_PER_REGION: u64 = 2;
        if region_size >= self.cfg.region_bucket_size.0 * MIN_BUCKET_COUNT_PER_REGION {
            let mut bucket_checker = size::Checker::new(
                self.cfg.region_bucket_size.0, /* not used */
                self.cfg.region_bucket_size.0, /* not used */
                region_size / self.cfg.region_bucket_size.0,
                CheckPolicy::Approximate,
            );
            return bucket_checker
                .approximate_split_keys(region, engine)
                .map(|keys| Bucket {
                    keys: keys
                        .into_iter()
                        .map(|k| ::keys::origin_key(&k).to_vec())
                        .collect(),
                    size: region_size,
                });
        }
        Ok(Bucket {
            keys: vec![],
            size: region_size,
        })
    }

    #[inline]
    pub fn add_checker(&mut self, checker: Box<dyn SplitChecker<E>>) {
        self.checkers.push(checker);
    }

    #[inline]
    pub fn enable_region_bucket(&self) -> bool {
        self.cfg.enable_region_bucket
    }

    #[inline]
    pub fn region_bucket_size(&self) -> u64 {
        self.cfg.region_bucket_size.0
    }
}

#[inline]
pub fn calc_split_keys_count(
    count_per_region: u64,
    split_threshold: u64,
    max_count_per_region: u64,
    batch_split_limit: u64,
) -> u64 {
    if count_per_region < max_count_per_region {
        return 0;
    }

    // split keys count is split count - 1
    // e.g. when split_threshold is 60 & max_count_per_region is 90
    //      if count_per_region is 60, return 0
    //      if count_per_region is 90, return 1
    //      if count_per_region is 121, return 1
    //      if count_per_region is 183, return 2
    std::cmp::min(
        std::cmp::max(
            ((count_per_region as f64 / split_threshold as f64).round() as u64).saturating_sub(1),
            count_per_region / max_count_per_region,
        ),
        batch_split_limit,
    )
}

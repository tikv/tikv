// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub mod peer;
pub mod query_stats;
pub mod region;

pub use self::{
    peer::{
        find_peer, find_peer_by_id, find_peer_mut, is_learner, new_learner_peer, new_peer,
        new_witness_peer, remove_peer,
    },
    query_stats::{is_read_query, QueryStats},
    region::{
        check_key_in_region, check_key_in_region_exclusive, check_key_in_region_inclusive,
        region_on_same_stores, region_on_stores,
    },
};

#[cfg(test)]
mod tests {
    use kvproto::metapb::Region;

    use super::*;

    #[test]
    fn test_on_same_store() {
        let cases = vec![
            (vec![2, 3, 4], vec![], vec![1, 2, 3], vec![], false),
            (vec![2, 3, 1], vec![], vec![1, 2, 3], vec![], true),
            (vec![2, 3, 4], vec![], vec![1, 2], vec![], false),
            (vec![1, 2, 3], vec![], vec![1, 2, 3], vec![], true),
            (vec![1, 3], vec![2, 4], vec![1, 2], vec![3, 4], false),
            (vec![1, 3], vec![2, 4], vec![1, 3], vec![], false),
            (vec![1, 3], vec![2, 4], vec![], vec![2, 4], false),
            (vec![1, 3], vec![2, 4], vec![3, 1], vec![4, 2], true),
        ];

        for (s1, s2, s3, s4, exp) in cases {
            let mut r1 = Region::default();
            for (store_id, peer_id) in s1.into_iter().zip(0..) {
                r1.mut_peers().push(new_peer(store_id, peer_id));
            }
            for (store_id, peer_id) in s2.into_iter().zip(0..) {
                r1.mut_peers().push(new_learner_peer(store_id, peer_id));
            }

            let mut r2 = Region::default();
            for (store_id, peer_id) in s3.into_iter().zip(10..) {
                r2.mut_peers().push(new_peer(store_id, peer_id));
            }
            for (store_id, peer_id) in s4.into_iter().zip(10..) {
                r2.mut_peers().push(new_learner_peer(store_id, peer_id));
            }
            let res = region_on_same_stores(&r1, &r2);
            assert_eq!(res, exp, "{:?} vs {:?}", r1, r2);
        }
    }

    #[test]
    fn test_check_region_on_store() {
        let cases = vec![
            (vec![1, 2, 3], vec![], vec![], true),
            (vec![2, 3, 1], vec![], vec![1], true),
            (vec![1, 3, 2], vec![], vec![2, 3], true),
            (vec![3, 2, 1], vec![], vec![4], false),
            (vec![1, 2, 3], vec![], vec![2, 4], true),
            (vec![1, 3], vec![2, 4], vec![2], true),
            (vec![1, 3], vec![2, 4], vec![2, 3], true),
            (vec![1, 3], vec![2], vec![4], false),
        ];

        for (s1, s2, target_stores, exp) in cases {
            let mut region = Region::default();
            for (store_id, peer_id) in s1.into_iter().zip(0..) {
                region.mut_peers().push(new_peer(store_id, peer_id));
            }
            for (store_id, peer_id) in s2.into_iter().zip(0..) {
                region.mut_peers().push(new_learner_peer(store_id, peer_id));
            }

            let res = region_on_stores(&region, &target_stores);
            assert_eq!(
                res, exp,
                "region {:?} exists on {:?}",
                region, target_stores
            );
        }
    }
}

// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

/// This mod provide some utility types and functions for resource control.

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::ops::{Bound,RangeBounds};

/// Like `..=val`(a.k.a. `RangeToInclusive`), but allows `val` being a reference
/// to DSTs.
struct RangeToInclusiveRef<'a, T: ?Sized>(&'a T);

impl<'a, T: ?Sized> RangeBounds<T> for RangeToInclusiveRef<'a, T> {
    fn start_bound(&self) -> Bound<&T> {
        Bound::Unbounded
    }

    fn end_bound(&self) -> Bound<&T> {
        Bound::Included(self.0)
    }
}

struct RangeToExclusiveRef<'a, T: ?Sized>(&'a T);

impl<'a, T: ?Sized> RangeBounds<T> for RangeToExclusiveRef<'a, T> {
    fn start_bound(&self) -> Bound<&T> {
        Bound::Unbounded
    }

    fn end_bound(&self) -> Bound<&T> {
        Bound::Excluded(self.0)
    }
}

#[derive(Default, Debug, Clone)]
pub struct SegmentMap<K: Ord, V>(BTreeMap<K, SegmentValue<K, V>>);

#[derive(Clone, Debug)]
pub struct SegmentValue<R, T> {
    pub range_end: R,
    pub item: T,
}

/// A container for holding ranges without overlapping.
/// supports fast(`O(log(n))`) query of overlapping and points in segments.
///
/// Maybe replace it with extended binary search tree or the real segment tree?
/// So it can contains overlapping segments.
pub type SegmentSet<T> = SegmentMap<T, ()>;

impl<K: Ord, V: Default> SegmentMap<K, V> {
    /// Try to add a element into the segment tree, with default value.
    /// (This is useful when using the segment tree as a `Set`, i.e.
    /// `SegmentMap<T, ()>`)
    ///
    /// - If no overlapping, insert the range into the tree and returns `true`.
    /// - If overlapping detected, do nothing and return `false`.
    pub fn add(&mut self, (start, end): (K, K)) -> bool {
        self.insert((start, end), V::default())
    }
}

impl<K: Ord, V> SegmentMap<K, V> {
    /// Remove all records in the map.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Like `add`, but insert a value associated to the key.
    pub fn insert(&mut self, (start, end): (K, K), value: V) -> bool {
        if self.is_overlapping((&start, &end)) {
            return false;
        }
        self.0.insert(
            start,
            SegmentValue {
                range_end: end,
                item: value,
            },
        );
        true
    }

    /// Find a segment with its associated value by the point.
    pub fn get_by_point<R>(&self, point: &R) -> Option<(&K, &K, &V)>
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.0
            .range(RangeToInclusiveRef(point))
            .next_back()
            .filter(|(_, end)| <K as Borrow<R>>::borrow(&end.range_end) > point)
            .map(|(k, v)| (k, &v.range_end, &v.item))
    }

    /// Like `get_by_point`, but omit the segment.
    pub fn get_value_by_point<R>(&self, point: &R) -> Option<&V>
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.get_by_point(point).map(|(_, _, v)| v)
    }

    /// Like `get_by_point`, but omit the segment.
    pub fn get_interval_by_point<R>(&self, point: &R) -> Option<(&K, &K)>
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.get_by_point(point).map(|(k, v, _)| (k, v))
    }

    pub fn find_overlapping<R>(&self, range: (&R, &R)) -> Option<(&K, &K, &V)>
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        // o: The Start Key.
        // e: The End Key.
        // +: The Boundary of Candidate Range.
        // |------+-s----+----e----|
        // Firstly, we check whether the start point is in some range.
        // if true, it must be overlapping.
        if let Some(overlap_with_start) = self.get_by_point(range.0) {
            return Some(overlap_with_start);
        }
        // |--s----+-----+----e----|
        // Otherwise, the possibility of being overlapping would be there are some sub
        // range of the queried range...
        // |--s----+----e----+-----|
        // ...Or the end key is contained by some Range.
        // For faster query, we merged the two cases together.
        let covered_by_the_range = self
             .0
             // When querying possibility of overlapping by end key,
             // we don't want the range [end key, ...) become a candidate.
             // (which is impossible to overlapping with the range)
             .range(RangeToExclusiveRef(range.1))
             .next_back()
             .filter(|(start, end)| {
                 <K as Borrow<R>>::borrow(&end.range_end) > range.1
                     || <K as Borrow<R>>::borrow(start) > range.0
             });
        covered_by_the_range.map(|(k, v)| (k, &v.range_end, &v.item))
    }

    /// Check whether the range is overlapping with any range in the segment
    /// tree.
    pub fn is_overlapping<R>(&self, range: (&R, &R)) -> bool
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.find_overlapping(range).is_some()
    }

    pub fn get_inner(&mut self) -> &mut BTreeMap<K, SegmentValue<K, V>> {
        &mut self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}



#[cfg(test)]
mod tests{
#[test]
fn test_segment_tree() {
    let mut tree = SegmentMap::default();
    assert!(tree.add((1, 4)));
    assert!(tree.add((4, 8)));
    assert!(tree.add((42, 46)));
    assert!(!tree.add((3, 8)));
    assert!(tree.insert((47, 88), "hello".to_owned()));
    assert_eq!(
        tree.get_value_by_point(&49).map(String::as_str),
        Some("hello")
    );
    assert_eq!(tree.get_interval_by_point(&3), Some((&1, &4)));
    assert_eq!(tree.get_interval_by_point(&7), Some((&4, &8)));
    assert_eq!(tree.get_interval_by_point(&90), None);
    assert!(tree.is_overlapping((&1, &3)));
    assert!(tree.is_overlapping((&7, &9)));
    assert!(!tree.is_overlapping((&8, &42)));
    assert!(!tree.is_overlapping((&9, &10)));
    assert!(tree.is_overlapping((&2, &10)));
    assert!(tree.is_overlapping((&0, &9999999)));
}
}
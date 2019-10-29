// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// FIXME: This should be put inside the raftstore component.

use std::borrow::Borrow;
use std::fmt::{self, Debug, Display};

use super::v2::*;
use codec::prelude::BufferWriter;

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct RaftPhysicalKey(pub Vec<u8>);

impl Debug for RaftPhysicalKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use hex::ToHex;
        self.0.as_slice().write_hex_upper(f)
    }
}

impl Display for RaftPhysicalKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl KeyLike for RaftPhysicalKey {}

impl BufferWriter for RaftPhysicalKey {
    #[inline]
    unsafe fn bytes_mut(&mut self, size: usize) -> &mut [u8] {
        self.0.bytes_mut(size)
    }

    #[inline]
    unsafe fn advance_mut(&mut self, count: usize) {
        self.0.advance_mut(count)
    }

    #[inline]
    fn write_bytes(&mut self, values: &[u8]) -> std::result::Result<(), Box<codec::Error>> {
        self.0.write_bytes(values)
    }
}

impl Borrow<RaftPhysicalKeySlice> for RaftPhysicalKey {
    fn borrow(&self) -> &RaftPhysicalKeySlice {
        self.as_physical_slice()
    }
}

impl PhysicalKey for RaftPhysicalKey {
    const PHYSICAL_PREFIX: &'static [u8] = crate::DATA_PREFIX_KEY;
    type Slice = RaftPhysicalKeySlice;

    #[inline]
    fn _new_from_vec(vec: Vec<u8>) -> Self {
        RaftPhysicalKey(vec)
    }

    #[inline]
    fn _vec_ref(&self) -> &Vec<u8> {
        &self.0
    }

    #[inline]
    fn _vec_mut(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }

    #[inline]
    fn _into_vec(self) -> Vec<u8> {
        self.0
    }
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct RaftPhysicalKeySlice(pub [u8]);

impl Debug for RaftPhysicalKeySlice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use hex::ToHex;
        self.as_physical_std_slice().write_hex_upper(f)
    }
}

impl Display for RaftPhysicalKeySlice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl KeyLike for RaftPhysicalKeySlice {}

impl RaftPhysicalKeySlice {
    #[inline]
    pub fn as_basic(&self) -> &BasicPhysicalKeySlice {
        BasicPhysicalKeySlice::from_physical_std_slice(self.as_physical_std_slice())
    }
}

impl PhysicalKeySlice for RaftPhysicalKeySlice {
    type OwnedKey = RaftPhysicalKey;

    #[inline]
    fn as_physical_std_slice(&self) -> &[u8] {
        &self.0
    }

    #[inline]
    fn from_physical_std_slice(s: &[u8]) -> &Self {
        assert!(s.starts_with(crate::DATA_PREFIX_KEY));
        unsafe { &*(s as *const [u8] as *const Self) }
    }

    #[inline]
    fn as_logical_slice(&self) -> &LogicalKeySlice {
        LogicalKeySlice::from_std_slice(&self.0[crate::DATA_PREFIX_KEY.len()..])
    }
}

impl ToPhysicalKeySlice<RaftPhysicalKeySlice> for RaftPhysicalKeySlice {
    // Any PhysicalKeySlice itself implements ToPhysicalKeySlice.
    type SliceOwner = ();

    #[inline]
    fn to_physical_slice_container(&self) -> PKContainer<'_, (), RaftPhysicalKeySlice> {
        let r = self as *const RaftPhysicalKeySlice;
        unsafe { PKContainer::new((), r) }
    }
}

impl ToPhysicalKeySlice<RaftPhysicalKeySlice> for RaftPhysicalKey {
    // Any PhysicalKey convert to its slice is zero cost and does not need to carry an
    // extra owned value.
    type SliceOwner = ();

    #[inline]
    fn to_physical_slice_container(&self) -> PKContainer<'_, (), RaftPhysicalKeySlice> {
        self.as_physical_slice().to_physical_slice_container()
    }
}

impl ToPhysicalKeySlice<BasicPhysicalKeySlice> for RaftPhysicalKeySlice {
    // A raft layer physical key can be passed directly to places that accept BasicPhysicalKeySlice
    // since they are all physical keys.
    type SliceOwner = ();

    #[inline]
    fn to_physical_slice_container(&self) -> PKContainer<'_, (), BasicPhysicalKeySlice> {
        self.as_basic().to_physical_slice_container()
    }
}

impl ToPhysicalKeySlice<BasicPhysicalKeySlice> for RaftPhysicalKey {
    type SliceOwner = ();

    #[inline]
    fn to_physical_slice_container(&self) -> PKContainer<'_, (), BasicPhysicalKeySlice> {
        self.as_physical_slice().to_physical_slice_container()
    }
}

impl ToPhysicalKeySlice<RaftPhysicalKeySlice> for super::types::Key {
    type SliceOwner = RaftPhysicalKey;

    #[inline]
    fn to_physical_slice_container(
        &self,
    ) -> PKContainer<'_, RaftPhysicalKey, RaftPhysicalKeySlice> {
        let physical_key = RaftPhysicalKey::alloc_from_logical_slice(self.as_logical_key_slice());
        let key_slice = physical_key.as_physical_slice() as *const RaftPhysicalKeySlice;
        // `physical_key` is not mutable and its data address will never change, so that
        // the `key_slice` reference is always valid.
        unsafe { PKContainer::new(physical_key, key_slice) }
    }
}

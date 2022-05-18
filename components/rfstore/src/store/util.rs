// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    fmt::{Debug, Display, Formatter},
};

use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, Bytes, BytesMut};
use kvproto::{metapb, raft_cmdpb::RaftCmdRequest};
use protobuf::Message;
use slog::{Key, Record, Serializer};
use tikv_util::{box_err, codec::bytes::decode_bytes, debug};

use crate::{Error, Result};

/// WARNING: `NORMAL_REQ_CHECK_VER` and `NORMAL_REQ_CHECK_CONF_VER` **MUST NOT** be changed.
/// The reason is the same as `admin_cmd_epoch_lookup`.
pub static NORMAL_REQ_CHECK_VER: bool = true;
pub static NORMAL_REQ_CHECK_CONF_VER: bool = false;

pub fn check_region_epoch(
    req: &RaftCmdRequest,
    region: &metapb::Region,
    include_region: bool,
) -> Result<()> {
    let (check_ver, check_conf_ver) = if !req.has_admin_request() {
        // for get/set/delete, we don't care conf_version.
        (NORMAL_REQ_CHECK_VER, NORMAL_REQ_CHECK_CONF_VER)
    } else {
        let epoch_state =
            raftstore::store::util::admin_cmd_epoch_lookup(req.get_admin_request().get_cmd_type());
        (epoch_state.check_ver, epoch_state.check_conf_ver)
    };

    if !check_ver && !check_conf_ver {
        return Ok(());
    }

    if !req.get_header().has_region_epoch() {
        return Err(box_err!("missing epoch!"));
    }

    let from_epoch = req.get_header().get_region_epoch();
    compare_region_epoch(
        from_epoch,
        region,
        check_conf_ver,
        check_ver,
        include_region,
    )
}

pub fn compare_region_epoch(
    from_epoch: &metapb::RegionEpoch,
    region: &metapb::Region,
    check_conf_ver: bool,
    check_ver: bool,
    include_region: bool,
) -> Result<()> {
    // We must check epochs strictly to avoid key not in region error.
    //
    // A 3 nodes TiKV cluster with merge enabled, after commit merge, TiKV A
    // tells TiDB with a epoch not match error contains the latest target Region
    // info, TiDB updates its region cache and sends requests to TiKV B,
    // and TiKV B has not applied commit merge yet, since the region epoch in
    // request is higher than TiKV B, the request must be denied due to epoch
    // not match, so it does not read on a stale snapshot, thus avoid the
    // KeyNotInRegion error.
    let current_epoch = region.get_region_epoch();
    if (check_conf_ver && from_epoch.get_conf_ver() != current_epoch.get_conf_ver())
        || (check_ver && from_epoch.get_version() != current_epoch.get_version())
    {
        debug!(
            "epoch not match";
            "region_id" => region.get_id(),
            "from_epoch" => ?from_epoch,
            "current_epoch" => ?current_epoch,
        );
        let regions = if include_region {
            vec![region.to_owned()]
        } else {
            vec![]
        };
        return Err(Error::EpochNotMatch(
            format!(
                "current epoch of region {} is {:?}, but you \
                 sent {:?}",
                region.get_id(),
                current_epoch,
                from_epoch
            ),
            regions,
        ));
    }

    Ok(())
}

#[inline]
pub fn check_store_id(req: &RaftCmdRequest, store_id: u64) -> Result<()> {
    let peer = req.get_header().get_peer();
    if peer.get_store_id() == store_id {
        Ok(())
    } else {
        Err(Error::StoreNotMatch {
            to_store_id: peer.get_store_id(),
            my_store_id: store_id,
        })
    }
}

#[inline]
pub fn check_term(req: &RaftCmdRequest, term: u64) -> Result<()> {
    let header = req.get_header();
    if header.get_term() == 0 || term <= header.get_term() + 1 {
        Ok(())
    } else {
        // If header's term is 2 verions behind current term,
        // leadership may have been changed away.
        Err(Error::StaleCommand)
    }
}

#[inline]
pub fn check_peer_id(req: &RaftCmdRequest, peer_id: u64) -> Result<()> {
    let header = req.get_header();
    if header.get_peer().get_id() == peer_id {
        Ok(())
    } else {
        Err(box_err!(
            "mismatch peer id {} != {}",
            header.get_peer().get_id(),
            peer_id
        ))
    }
}

/// Check if key in region range (`start_key`, `end_key`).
pub fn check_key_in_region_exclusive(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if start_key < key && (key < end_key || end_key.is_empty()) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

/// Check if key in region range [`start_key`, `end_key`].
pub fn check_key_in_region_inclusive(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if key >= start_key && (end_key.is_empty() || key <= end_key) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

/// Check if key in region range [`start_key`, `end_key`).
pub fn check_key_in_region(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if key >= start_key && (end_key.is_empty() || key < end_key) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

pub fn cf_name_to_num(cf_name: &str) -> usize {
    match cf_name {
        "write" => 0,
        "lock" => 1,
        "extra" => 2,
        _ => 0,
    }
}

/// Parse data of entry `index`.
///
/// # Panics
///
/// If `data` is corrupted, this function will panic.
// TODO: make sure received entries are not corrupted
#[inline]
pub fn parse_data_at<T: Message + Default>(data: &[u8], index: u64, tag: RegionIDVer) -> T {
    let mut result = T::default();
    result.merge_from_bytes(data).unwrap_or_else(|e| {
        panic!("{} data is corrupted at {}: {:?}", tag, index, e);
    });
    result
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct RegionIDVer {
    id: u64,
    ver: u64,
}

impl RegionIDVer {
    pub fn new(id: u64, ver: u64) -> Self {
        Self { id, ver }
    }

    pub fn from_region(region: &metapb::Region) -> Self {
        Self::new(region.get_id(), region.get_region_epoch().get_version())
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn ver(&self) -> u64 {
        self.ver
    }
}

impl slog::Value for RegionIDVer {
    fn serialize(
        &self,
        _record: &Record<'_>,
        key: Key,
        serializer: &mut dyn Serializer,
    ) -> slog::Result {
        serializer.emit_str(key, &self.to_string())
    }
}

impl Display for RegionIDVer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "[{}:{}]", self.id, self.ver)
    }
}

pub(crate) const RAFT_STATE_KEY_BYTE: u8 = 1;
pub(crate) const REGION_META_KEY_BYTE: u8 = 2;
pub(crate) const REGION_META_KEY_PREFIX: &[u8] = &[REGION_META_KEY_BYTE];
pub(crate) const STORE_IDENT_KEY: &[u8] = &[3];
pub(crate) const PREPARE_BOOTSTRAP_KEY: &[u8] = &[4];
pub(crate) const KV_ENGINE_META_KEY: &[u8] = &[5];
pub(crate) const EMPTY_KEY: &[u8] = &[];
pub(crate) const RAW_INITIAL_START_KEY: &[u8] = &[2];
pub(crate) const RAW_INITIAL_END_KEY: &[u8] = &[255, 255, 255, 255, 255, 255, 255, 255];

pub(crate) fn raft_state_key(version: u64) -> Bytes {
    let mut key = BytesMut::with_capacity(5);
    key.put_u8(RAFT_STATE_KEY_BYTE);
    key.put_u32(version as u32);
    key.freeze()
}

pub(crate) fn region_state_key(version: u64, conf_ver: u64) -> Bytes {
    let mut key = BytesMut::with_capacity(9);
    key.put_u8(REGION_META_KEY_BYTE);
    key.put_u32(version as u32);
    key.put_u32(conf_ver as u32);
    key.freeze()
}

pub(crate) fn parse_region_state_key(key: &[u8]) -> (u64, u64) {
    let ver = BigEndian::read_u32(&key[1..]);
    let conf_ver = BigEndian::read_u32(&key[5..]);
    (ver as u64, conf_ver as u64)
}

// Get the `start_key` of current region in raw form.
pub(crate) fn raw_start_key(region: &metapb::Region) -> Bytes {
    // only initialized region's start_key can be encoded, otherwise there must be bugs
    // somewhere.
    if region.start_key.is_empty() {
        // Data starts with 0x01 is used as local key.
        return Bytes::from_static(RAW_INITIAL_START_KEY);
    }
    let mut slice = region.start_key.as_slice();
    let start_key = decode_bytes(&mut slice, false).unwrap();
    Bytes::from(start_key)
}

// Get the `end_key` of current region in raw form.
pub(crate) fn raw_end_key(region: &metapb::Region) -> Bytes {
    // only initialized region's end_key can be encoded, otherwise there must be bugs
    // somewhere.
    if region.end_key.is_empty() {
        return Bytes::from_static(RAW_INITIAL_END_KEY);
    }
    let mut slice = region.end_key.as_slice();
    let end_key = decode_bytes(&mut slice, false).unwrap();
    Bytes::from(end_key)
}

// Get the `end_key` of current region in raw form.
pub(crate) fn decode_end_key(mut end_key: &[u8]) -> Bytes {
    // only initialized region's end_key can be encoded, otherwise there must be bugs
    // somewhere.
    if end_key.is_empty() {
        return Bytes::from_static(RAW_INITIAL_END_KEY);
    }
    let end_key = decode_bytes(&mut end_key, false).unwrap();
    Bytes::from(end_key)
}

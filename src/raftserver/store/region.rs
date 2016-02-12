use proto::metapb;
use super::engine::Mutator;
use super::keys;
use raftserver::{Result, other};

// How do we organize the route meta key to determine the region a key belongs to?
//
// We use 2 hash (meta1, meta2) to route the key.
// Meta1 is always located in region1 and can't be split. Meta2 can be split into
// different regions.
// For a given key, first we find in meta1 to know which meta2 has this key,
// then find in meta2 and know where the really data region is.
//
// The region route meta key for a region is encoded with a meta prefix + region end key.
// E,g, if a data region key range is ["123", "999"), the relevant meta2 key is
//  meta2 prefix + "999"
// Why using the end key but not start key? We use seek in RocksDB to search the key, if we have
// 2 regions ["111", "222"), ["222", "333") and encode the key with start key, the layout may be:
//  \x00\x03"111" -> region 2
//  \x00\x03"222" -> region 3
// If we want to find key "123", using seek we may get `\x00\x03"222"`, but not `\x00\x03"111"`,
// (Rocksdb seeks the key >= given key), so we must move back to get the correct region.

trait RouteMetaAction <T: Mutator>{
    fn handle(&self, w: &T, key: &[u8], region: &metapb::Region) -> Result<()>;
}

struct PutRouteMetaAction;

impl<T: Mutator> RouteMetaAction<T> for PutRouteMetaAction {
    fn handle(&self, w: &T, key: &[u8], region: &metapb::Region) -> Result<()> {
        try!(w.put_msg(key, region));
        Ok(())
    }
}

struct DeleteRouteMetaAction;

impl<T: Mutator> RouteMetaAction<T> for DeleteRouteMetaAction {
    fn handle(&self, w: &T, key: &[u8], _: &metapb::Region) -> Result<()> {
        try!(w.delete(key));
        Ok(())
    }
}

fn put_route_meta<T: Mutator>(w: &T, key: &[u8], region: &metapb::Region) -> Result<()> {
    try!(w.put_msg(key, region));
    Ok(())
}

fn del_route_meta<T: Mutator>(w: &T, key: &[u8], _: &metapb::Region) -> Result<()> {
    try!(w.delete(key));
    Ok(())
}

pub fn split_region_route<T: Mutator>(w: &T,
                                      left: &metapb::Region,
                                      right: &metapb::Region)
                                      -> Result<()> {
    try!(handle_region_route(w, left, PutRouteMetaAction));
    try!(handle_region_route(w, right, PutRouteMetaAction));
    Ok(())
}

pub fn update_region_route<T: Mutator>(w: &T, region: &metapb::Region) -> Result<()> {
    try!(handle_region_route(w, region, PutRouteMetaAction));
    Ok(())
}

fn handle_region_route<T: Mutator, F: RouteMetaAction<T>>(w: &T,
                                                          region: &metapb::Region,
                                                          action: F)
                                                          -> Result<()> {
    let start_key = region.get_start_key();
    let end_key = region.get_end_key();

    // We can't split meta1.
    if end_key.starts_with(keys::META1_PREFIX_KEY) ||
       start_key.starts_with(keys::META1_PREFIX_KEY) {
        return Err(other("meta1 route can't be split"));
    }

    // If regions end_key ends with a meta2 prefix, updates relevant meta1.
    if end_key.starts_with(keys::META2_PREFIX_KEY) {
        try!(action.handle(w, &keys::region_route_meta_key(end_key), region));
    } else {
        // The regions ends with a normal data key, updates relevant meta2.
        try!(action.handle(w, &vec![keys::META2_PREFIX_KEY, end_key].concat(), region));

        // The regions start_key starts with MIN_KEY or meta2 prefix, updates meta1 KEY_MAX.
        if start_key == keys::MIN_KEY || start_key.starts_with(keys::META2_PREFIX_KEY) {
            try!(action.handle(w,
                               &vec![keys::META1_PREFIX_KEY, keys::MAX_KEY].concat(),
                               region));
        }
    }

    Ok(())
}

// Checks whether the key to split the route is valid or not.
pub fn validate_route_meta_split_key(key: &[u8]) -> bool {
    // TODO: should we consider META2_MAX_KEY as invalid key?
    if key == keys::META2_MAX_KEY {
        return false;
    }

    // Meta1 can't be split.
    if key >= keys::MIN_KEY && key < keys::META2_PREFIX_KEY {
        return false;
    }

    return true;

}

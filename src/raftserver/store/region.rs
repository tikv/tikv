use proto::metapb;
use super::engine::Mutator;
use super::keys;
use raftserver::{Result, other};

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

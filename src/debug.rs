use kvproto::kvrpcpb::KeyRange;
use txn_types::Key;
use tidb_query_datatype::codec::table;

pub fn has_user_table_key(ranges: &[KeyRange]) -> bool {
    ranges.iter().any(|r| is_user_table_key(r.get_start_key())) || ranges.iter().any(|r| is_user_table_key(r.get_end_key()))
}

pub fn is_user_table_key(key: &[u8]) -> bool {
    table::decode_table_id(key).ok().filter(|tid| *tid > 100 && *tid < 1000000).is_some()
}

pub fn extract_user_table_id(key: &Key) -> Option<i64> {
    key.to_raw().ok().and_then(|raw| table::decode_table_id(raw.as_slice()).ok()).filter(|tid| *tid > 100 && *tid < 1000000)
}

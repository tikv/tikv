// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub mod simple_write;

use protobuf::Message;
use tikv_util::slog_panic;

pub fn parse_at<M: Message + Default>(
    logger: &slog::Logger,
    buf: &[u8],
    index: u64,
    term: u64,
) -> M {
    let mut m = M::default();
    match m.merge_from_bytes(buf) {
        Ok(()) => m,
        Err(e) => slog_panic!(
            logger,
            "data is corrupted";
            "term" => term,
            "index" => index,
            "error" => ?e,
        ),
    }
}

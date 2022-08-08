// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod test_compaction;

fn i_to_key(i: usize) -> Vec<u8> {
    format!("key{:08}", i).into_bytes()
}

fn i_to_val(i: usize) -> Vec<u8> {
    format!("val{:08}", i).into_bytes().repeat(10)
}

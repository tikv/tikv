// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

pub const JSON_LITERAL_NIL: u8 = 0x00;
pub const JSON_LITERAL_TRUE: u8 = 0x01;
pub const JSON_LITERAL_FALSE: u8 = 0x02;

pub const TYPE_LEN: usize = 1;
pub const LITERAL_LEN: usize = 1;
pub const U16_LEN: usize = 2;
pub const U32_LEN: usize = 4;
pub const NUMBER_LEN: usize = 8;
pub const HEADER_LEN: usize = ELEMENT_COUNT_LEN + SIZE_LEN; // element size + data size
pub const KEY_ENTRY_LEN: usize = U32_LEN + U16_LEN;
pub const VALUE_ENTRY_LEN: usize = TYPE_LEN + U32_LEN;
pub const ELEMENT_COUNT_LEN: usize = U32_LEN;
pub const SIZE_LEN: usize = U32_LEN;

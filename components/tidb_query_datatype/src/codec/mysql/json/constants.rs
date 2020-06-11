// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// Inlined json literal value
pub const JSON_LITERAL_NIL: u8 = 0x00;
pub const JSON_LITERAL_TRUE: u8 = 0x01;
pub const JSON_LITERAL_FALSE: u8 = 0x02;

// Binary json constants
pub const TYPE_LEN: usize = 1;
pub const LITERAL_LEN: usize = 1;
pub const U16_LEN: usize = 2;
pub const U32_LEN: usize = 4;
pub const NUMBER_LEN: usize = 8;
pub const HEADER_LEN: usize = ELEMENT_COUNT_LEN + SIZE_LEN; // element size + data size
pub const KEY_OFFSET_LEN: usize = U32_LEN;
pub const KEY_LEN_LEN: usize = U16_LEN;
pub const KEY_ENTRY_LEN: usize = KEY_OFFSET_LEN + KEY_LEN_LEN;
pub const VALUE_ENTRY_LEN: usize = TYPE_LEN + U32_LEN;
pub const ELEMENT_COUNT_LEN: usize = U32_LEN;
pub const SIZE_LEN: usize = U32_LEN;

// Type precedence for json comparison
pub const PRECEDENCE_BLOB: i32 = -1;
pub const PRECEDENCE_BIT: i32 = -2;
pub const PRECEDENCE_OPAQUE: i32 = -3;
pub const PRECEDENCE_DATETIME: i32 = -4;
pub const PRECEDENCE_TIME: i32 = -5;
pub const PRECEDENCE_DATE: i32 = -6;
pub const PRECEDENCE_BOOLEAN: i32 = -7;
pub const PRECEDENCE_ARRAY: i32 = -8;
pub const PRECEDENCE_OBJECT: i32 = -9;
pub const PRECEDENCE_STRING: i32 = -10;
pub const PRECEDENCE_NUMBER: i32 = -11;
pub const PRECEDENCE_NULL: i32 = -12;

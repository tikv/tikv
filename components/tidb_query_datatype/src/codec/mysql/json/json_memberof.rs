// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use super::{super::Result, JsonRef, JsonType};

impl<'a> JsonRef<'a> {
    /// `json_contains` is the implementation for JSON_CONTAINS in mysql
    /// <https://dev.mysql.com/doc/refman/5.7/en/json-search-functions.html#function_json-contains>
    /// See `ContainsBinaryJSON()` in TiDB `types/json_binary_functions.go`
    pub fn member_of(&self, target: JsonRef<'_>) -> Result<bool> {
        match self.type_code {
            JsonType::Array => {
                let elem_count = self.get_elem_count();
                for i in 0..elem_count {
                    if self.array_get_elem(i)?.partial_cmp(&target).unwrap() == Ordering::Equal {
                        return Ok(true);
                    }
                }
            }
            _ => {
                if self.partial_cmp(&target).unwrap() == Ordering::Equal {
                    return Ok(true);
                }
            }
        };
        Ok(false)
    }
}

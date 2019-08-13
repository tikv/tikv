// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod chunk;
mod column;

pub use crate::codec::{Error, Result};

pub use self::chunk::{Chunk, ChunkEncoder};

#[cfg(test)]
mod tests {
    use tidb_query_datatype::FieldTypeAccessor;
    use tidb_query_datatype::FieldTypeTp;
    use tipb::FieldType;

    pub fn field_type(tp: FieldTypeTp) -> FieldType {
        let mut fp = FieldType::default();
        fp.as_mut_accessor().set_tp(tp);
        fp
    }
}

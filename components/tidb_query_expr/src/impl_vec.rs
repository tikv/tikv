// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::codec::data_type::*;

#[rpn_fn(writer)]
#[inline]
fn vec_as_text(a: VectorFloat32Ref, writer: BytesWriter) -> Result<BytesGuard> {
    Ok(writer.write(Some(Bytes::from(a.to_string()))))
}

#[rpn_fn]
#[inline]
fn vec_dims(arg: VectorFloat32Ref) -> Result<Option<Int>> {
    Ok(Some(arg.len() as Int))
}

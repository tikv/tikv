// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::errorpb::Error as ProtoError;
use engine_traits::Error as EngineTraitsError;

pub trait IntoProtoError {
    fn into_protoerror(self) -> ProtoError;
}

impl IntoProtoError for EngineTraitsError {
    fn into_protoerror(self) -> ProtoError {
        let mut errorpb = ProtoError::default();
        errorpb.set_message(format!("{}", self));

        if let EngineTraitsError::NotInRange(key, region_id, start_key, end_key) = self {
            errorpb.mut_key_not_in_region().set_key(key);
            errorpb.mut_key_not_in_region().set_region_id(region_id);
            errorpb.mut_key_not_in_region().set_start_key(start_key);
            errorpb.mut_key_not_in_region().set_end_key(end_key);
        }

        errorpb
    }    
}

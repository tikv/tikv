// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Various conversions between types that can't have their own
// interdependencies. These are used to convert between error_traits::Error and
// other Error's that error_traits can't depend on.

use engine_traits::Error as EngineTraitsError;
use kvproto::errorpb::Error as ProtoError;
use raft::Error as RaftError;

pub trait IntoOther<O> {
    fn into_other(self) -> O;
}

impl IntoOther<ProtoError> for EngineTraitsError {
    fn into_other(self) -> ProtoError {
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

impl IntoOther<RaftError> for EngineTraitsError {
    fn into_other(self) -> RaftError {
        RaftError::Store(raft::StorageError::Other(self.into()))
    }
}

pub fn into_other<F, T>(e: F) -> T
where F: IntoOther<T>
{
    e.into_other()    
}

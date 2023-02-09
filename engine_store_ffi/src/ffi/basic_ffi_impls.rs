// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::pin::Pin;

use super::interfaces_ffi::BaseBuffView;

impl From<&[u8]> for BaseBuffView {
    fn from(s: &[u8]) -> Self {
        let ptr = s.as_ptr() as *const _;
        Self {
            data: ptr,
            len: s.len() as u64,
        }
    }
}

#[allow(clippy::clone_on_copy)]
impl Clone for BaseBuffView {
    fn clone(&self) -> BaseBuffView {
        BaseBuffView {
            data: self.data.clone(),
            len: self.len.clone(),
        }
    }
}

impl BaseBuffView {
    pub fn to_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data as *const _, self.len as usize) }
    }
}
pub struct ProtoMsgBaseBuff {
    data: Vec<u8>,
}

impl ProtoMsgBaseBuff {
    pub fn new<T: protobuf::Message>(msg: &T) -> Self {
        ProtoMsgBaseBuff {
            data: msg.write_to_bytes().unwrap(),
        }
    }
}

impl From<Pin<&ProtoMsgBaseBuff>> for BaseBuffView {
    fn from(p: Pin<&ProtoMsgBaseBuff>) -> Self {
        Self {
            data: p.data.as_ptr() as *const _,
            len: p.data.len() as u64,
        }
    }
}

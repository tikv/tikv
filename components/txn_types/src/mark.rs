// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{Error, ErrorInner, Result, TimeStamp};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MarkType {
    Lock = b'L',
    Rollback = b'R',
}

impl TryFrom<u8> for MarkType {
    type Error = Error;

    fn try_from(v: u8) -> Result<Self> {
        match v {
            b'L' => Ok(MarkType::Lock),
            b'R' => Ok(MarkType::Rollback),
            _ => Err(Error::from(ErrorInner::BadFormatWrite)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Mark {
    pub mark_type: MarkType,
    pub commit_ts: TimeStamp,
}

impl Mark {
    pub fn parse(mut b: &[u8]) -> Result<Mark> {
        let mark_type = b
            .read_u8()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))
            .and_then(MarkType::try_from)?;
        let commit_ts = b
            .read_u64::<LittleEndian>()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?
            .into();
        Ok(Mark {
            mark_type,
            commit_ts,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(9);
        b.push(self.mark_type as u8);
        b.write_u64::<LittleEndian>(self.commit_ts.into_inner())
            .unwrap();
        b
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mark_type_try_from_u8() {
        assert_eq!(
            MarkType::try_from(MarkType::Lock as u8).unwrap(),
            MarkType::Lock
        );
        assert_eq!(
            MarkType::try_from(MarkType::Rollback as u8).unwrap(),
            MarkType::Rollback
        );
        MarkType::try_from(255).unwrap_err();
    }

    #[test]
    fn test_serde_mark() {
        let mark = Mark {
            mark_type: MarkType::Lock,
            commit_ts: TimeStamp::zero(),
        };
        let b = mark.to_bytes();
        assert_eq!(b.len(), 9);
        assert_eq!(Mark::parse(&b).unwrap(), mark);
    }
}

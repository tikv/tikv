use crate::storage::mvcc::WriteType;
use crate::storage::SHORT_VALUE_PREFIX;

pub struct Write2<'a> {
    pub write_type: WriteType,
    pub start_ts: u64,
    pub short_value: Option<&'a [u8]>,
}

impl<'a> Write2<'a> {
    pub fn from_slice(mut b: &'a [u8]) -> failure::Fallible<Write2<'a>> {
        use codec::prelude::NumberDecoder;

        if b.is_empty() {
            return Err(format_err!("Invalid write"));
        }
        let write_type =
            WriteType::from_u8(b.read_u8()?).ok_or_else(|| format_err!("Invalid write"))?;
        let start_ts = b.read_var_u64()?;
        if b.is_empty() {
            return Ok(Write2 {
                write_type,
                start_ts,
                short_value: None,
            });
        }

        let flag = b.read_u8()?;
        assert_eq!(flag, SHORT_VALUE_PREFIX, "invalid flag [{}] in write", flag);

        let len = b.read_u8()?;
        assert_eq!(
            b.len(),
            len as usize,
            "short value len [{}] not equal to content len [{}]",
            len,
            b.len()
        );

        Ok(Write2 {
            write_type,
            start_ts,
            short_value: Some(b),
        })
    }
}

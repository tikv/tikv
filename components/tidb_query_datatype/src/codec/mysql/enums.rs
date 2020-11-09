use std::cmp::Ordering;
use std::sync::Arc;
use tikv_util::buffer_vec::BufferVec;

#[derive(Clone, Debug)]
pub struct Enum {
    data: Arc<BufferVec>,

    // MySQL Enum is 1-based index, value == 0 means this enum is ''
    value: usize,
}

impl Enum {
    pub fn new(data: Arc<BufferVec>, value: usize) -> Self {
        Self { data, value }
    }
    pub fn value(&self) -> usize {
        self.value
    }
}

impl ToString for Enum {
    fn to_string(&self) -> String {
        if self.value == 0 {
            return String::new();
        }

        let buf = &self.data[self.value - 1];

        // TODO: Check the requirements and intentions of to_string usage.
        String::from_utf8_lossy(buf).to_string()
    }
}

impl Eq for Enum {}

impl PartialEq for Enum {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Ord for Enum {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl PartialOrd for Enum {
    fn partial_cmp(&self, right: &Self) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

impl crate::codec::data_type::AsMySQLBool for Enum {
    #[inline]
    fn as_mysql_bool(
        &self,
        _context: &mut crate::expr::EvalContext,
    ) -> tidb_query_common::error::Result<bool> {
        Ok(self.value != 0)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct EnumRef<'a> {
    data: &'a BufferVec,
    value: usize,
}

impl<'a> EnumRef<'a> {
    pub fn new(data: &'a BufferVec, value: usize) -> Self {
        Self { data, value }
    }
}

impl<'a> Eq for EnumRef<'a> {}

impl<'a> PartialEq for EnumRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<'a> Ord for EnumRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl<'a> PartialOrd for EnumRef<'a> {
    fn partial_cmp(&self, right: &Self) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_string() {
        let cases = vec![(vec!["a", "b", "c"], 1, "a"), (vec!["a", "b", "c"], 3, "c")];

        for (data, value, expect) in cases {
            let mut buf = BufferVec::new();
            for v in data {
                buf.push(v)
            }

            let e = Enum {
                data: Arc::new(buf),
                value,
            };

            assert_eq!(e.to_string(), expect.to_string())
        }
    }
}

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;

#[derive(Debug, Default, Clone)]
pub struct TtlProperties {
    pub max_expire_ts: Option<u64>,
    pub min_expire_ts: Option<u64>,
}

impl TtlProperties {
    pub fn add(&mut self, expire_ts: u64) {
        self.merge(&TtlProperties {
            max_expire_ts: Some(expire_ts),
            min_expire_ts: Some(expire_ts),
        });
    }

    pub fn merge(&mut self, other: &TtlProperties) {
        if let Some(max_expire_ts) = other.max_expire_ts {
            self.max_expire_ts = Some(std::cmp::max(
                self.max_expire_ts.unwrap_or(u64::MIN),
                max_expire_ts,
            ));
        }
        if let Some(min_expire_ts) = other.min_expire_ts {
            self.min_expire_ts = Some(std::cmp::min(
                self.min_expire_ts.unwrap_or(u64::MAX),
                min_expire_ts,
            ));
        }
    }

    pub fn is_some(&self) -> bool {
        self.max_expire_ts.is_some() || self.min_expire_ts.is_some()
    }

    pub fn is_none(&self) -> bool {
        !self.is_some()
    }
}

pub trait TtlPropertiesExt {
    fn get_range_ttl_properties_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(String, TtlProperties)>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ttl_properties() {
        let verify = |prop: &TtlProperties, min: Option<u64>, max: Option<u64>| {
            assert_eq!(prop.min_expire_ts, min);
            assert_eq!(prop.max_expire_ts, max);
        };

        // add
        let mut prop1 = TtlProperties::default();
        assert!(prop1.is_none());
        prop1.add(10);
        assert!(prop1.is_some());
        verify(&prop1, Some(10), Some(10));

        // merge
        {
            let mut prop2 = TtlProperties::default();
            prop2.add(20);
            verify(&prop2, Some(20), Some(20));

            prop1.merge(&prop2);
            verify(&prop1, Some(10), Some(20));
        }

        // none merge some
        let mut prop3 = TtlProperties::default();
        prop3.merge(&prop1);
        verify(&prop3, Some(10), Some(20));

        // some merge none
        {
            let prop4 = TtlProperties::default();
            prop3.merge(&prop4);
            verify(&prop3, Some(10), Some(20));
        }

        // add
        {
            prop3.add(30);
            verify(&prop3, Some(10), Some(30));
            prop3.add(0);
            verify(&prop3, Some(0), Some(30));
        }
    }
}

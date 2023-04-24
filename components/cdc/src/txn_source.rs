// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

// The bitmap:
// |RESERVED|LOSSY_DDL_REORG_SOURCE_BITS|CDC_WRITE_SOURCE_BITS|
// |  48    |             8             | 4(RESERVED) |  4    |
//
// TiCDC uses 1 - 255 to indicate the source of TiDB.
// For now, 1 - 15 are reserved for TiCDC to implement BDR synchronization.
// 16 - 255 are reserved for extendability.
const CDC_WRITE_SOURCE_BITS: u64 = 8;
const CDC_WRITE_SOURCE_MAX: u64 = (1 << CDC_WRITE_SOURCE_BITS) - 1;

// TiCDC uses 1-255 to indicate the change from a lossy DDL reorg Backfill job.
// For now, we only use 1 for column reorg backfill job.
#[cfg(test)]
const LOSSY_DDL_REORG_SOURCE_BITS: u64 = 8;
#[cfg(test)]
const LOSSY_DDL_COLUMN_REORG_SOURCE: u64 = 1;
#[cfg(test)]
const LOSSY_DDL_REORG_SOURCE_MAX: u64 = (1 << LOSSY_DDL_REORG_SOURCE_BITS) - 1;
const LOSSY_DDL_REORG_SOURCE_SHIFT: u64 = CDC_WRITE_SOURCE_BITS;

/// For kv.TxnSource
/// We use an uint64 to represent the source of a transaction.
/// The first 8 bits are reserved for TiCDC, and the next 8 bits are reserved
/// for Lossy DDL reorg Backfill job. The remaining 48 bits are reserved for
/// extendability.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) struct TxnSource(u64);

impl TxnSource {
    #[cfg(test)]
    pub(crate) fn set_cdc_write_source(&mut self, value: u64) {
        if value > CDC_WRITE_SOURCE_MAX {
            unreachable!("Only use it in tests")
        }
        self.0 |= value;
    }

    #[cfg(test)]
    pub(crate) fn get_cdc_write_source(&self) -> u64 {
        self.0 & CDC_WRITE_SOURCE_MAX
    }

    pub(crate) fn is_cdc_write_source_set(txn_source: u64) -> bool {
        (txn_source & CDC_WRITE_SOURCE_MAX) != 0
    }

    #[cfg(test)]
    pub(crate) fn set_lossy_ddl_reorg_source(&mut self, value: u64) {
        if value > LOSSY_DDL_REORG_SOURCE_MAX {
            unreachable!("Only use it in tests")
        }
        self.0 |= value << LOSSY_DDL_REORG_SOURCE_SHIFT;
    }

    #[cfg(test)]
    pub(crate) fn get_lossy_ddl_reorg_source(&self) -> u64 {
        (self.0 >> LOSSY_DDL_REORG_SOURCE_SHIFT) & LOSSY_DDL_REORG_SOURCE_MAX
    }

    pub(crate) fn is_lossy_ddl_reorg_source_set(txn_source: u64) -> bool {
        (txn_source >> LOSSY_DDL_REORG_SOURCE_SHIFT) != 0
    }
}

impl From<TxnSource> for u64 {
    fn from(val: TxnSource) -> Self {
        val.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_cdc_write_source() {
        let mut txn_source = TxnSource::default();
        txn_source.set_cdc_write_source(1);
        assert_eq!(txn_source.get_cdc_write_source(), 1);
    }

    #[test]
    fn test_is_cdc_write_source_set() {
        let mut txn_source = TxnSource::default();
        txn_source.set_cdc_write_source(1);
        assert_eq!(TxnSource::is_cdc_write_source_set(txn_source.0), true);

        let txn_source = TxnSource::default();
        assert_eq!(TxnSource::is_cdc_write_source_set(txn_source.0), false);
    }

    #[test]
    fn test_get_lossy_ddl_reorg_source() {
        let mut txn_source = TxnSource::default();
        txn_source.set_lossy_ddl_reorg_source(LOSSY_DDL_COLUMN_REORG_SOURCE);
        assert_eq!(
            txn_source.get_lossy_ddl_reorg_source(),
            LOSSY_DDL_COLUMN_REORG_SOURCE
        );
    }

    #[test]
    fn test_is_lossy_ddl_reorg_source_set() {
        let mut txn_source = TxnSource::default();
        txn_source.set_lossy_ddl_reorg_source(LOSSY_DDL_COLUMN_REORG_SOURCE);
        assert_eq!(TxnSource::is_lossy_ddl_reorg_source_set(txn_source.0), true);

        let txn_source = TxnSource::default();
        assert_eq!(
            TxnSource::is_lossy_ddl_reorg_source_set(txn_source.0),
            false
        );
    }
}

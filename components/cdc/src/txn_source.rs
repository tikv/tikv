// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

// We use an uint64 to represent the source of a transaction.
// The first 8 bits are reserved for TiCDC to implement BDR synchronization, and
// the next 4 bits are reserved for Lossy DDL reorg Backfill job. The remaining
// 52 bits are reserved for extendability.
const CDC_WRITE_SOURCE_BITS: u64 = 8;
const CDC_WRITE_SOURCE_MAX: u64 = (1 << CDC_WRITE_SOURCE_BITS) - 1;

#[cfg(test)]
const LOSSY_DDL_REORG_SOURCE_BITS: u64 = 4;
#[cfg(test)]
const LOSSY_DDL_COLUMN_REORG_SOURCE: u64 = 1;
#[cfg(test)]
const LOSSY_DDL_REORG_SOURCE_MAX: u64 = (1 << LOSSY_DDL_REORG_SOURCE_BITS) - 1;
const LOSSY_DDL_REORG_SOURCE_SHIFT: u64 = CDC_WRITE_SOURCE_BITS;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TxnSource(u64);

impl TxnSource {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        TxnSource(0)
    }

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

impl Into<u64> for TxnSource {
    fn into(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_cdc_write_source() {
        let mut txn_source = TxnSource::new();
        txn_source.set_cdc_write_source(1);
        assert_eq!(txn_source.get_cdc_write_source(), 1);
    }

    #[test]
    fn test_is_cdc_write_source_set() {
        let mut txn_source = TxnSource::new();
        txn_source.set_cdc_write_source(1);
        assert_eq!(TxnSource::is_cdc_write_source_set(txn_source.0), true);

        let txn_source = TxnSource::new();
        assert_eq!(TxnSource::is_cdc_write_source_set(txn_source.0), false);
    }

    #[test]
    fn test_get_lossy_ddl_reorg_source() {
        let mut txn_source = TxnSource::new();
        txn_source.set_lossy_ddl_reorg_source(LOSSY_DDL_COLUMN_REORG_SOURCE);
        assert_eq!(
            txn_source.get_lossy_ddl_reorg_source(),
            LOSSY_DDL_COLUMN_REORG_SOURCE
        );
    }

    #[test]
    fn test_is_lossy_ddl_reorg_source_set() {
        let mut txn_source = TxnSource::new();
        txn_source.set_lossy_ddl_reorg_source(LOSSY_DDL_COLUMN_REORG_SOURCE);
        assert_eq!(TxnSource::is_lossy_ddl_reorg_source_set(txn_source.0), true);

        let txn_source = TxnSource::new();
        assert_eq!(
            TxnSource::is_lossy_ddl_reorg_source_set(txn_source.0),
            false
        );
    }
}

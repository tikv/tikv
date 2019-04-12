// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::{i64, mem, u64};

use tipb::select;

use super::{Error, Result};
use crate::coprocessor::codec::mysql::Tz;

/// Flags are used by `DAGRequest.flags` to handle execution mode, like how to handle
/// truncate error.
/// `FLAG_IGNORE_TRUNCATE` indicates if truncate error should be ignored.
/// Read-only statements should ignore truncate error, write statements should not ignore
/// truncate error.
pub const FLAG_IGNORE_TRUNCATE: u64 = 1;
/// `FLAG_TRUNCATE_AS_WARNING` indicates if truncate error should be returned as warning.
/// This flag only matters if `FLAG_IGNORE_TRUNCATE` is not set, in strict sql mode, truncate error
/// should be returned as error, in non-strict sql mode, truncate error should be saved as warning.
pub const FLAG_TRUNCATE_AS_WARNING: u64 = 1 << 1;
// `FLAG_PAD_CHAR_TO_FULL_LENGTH` indicates if sql_mode 'PAD_CHAR_TO_FULL_LENGTH' is set.
pub const FLAG_PAD_CHAR_TO_FULL_LENGTH: u64 = 1 << 2;
/// `FLAG_IN_INSERT_STMT` indicates if this is a INSERT statement.
pub const FLAG_IN_INSERT_STMT: u64 = 1 << 3;
/// `FLAG_IN_UPDATE_OR_DELETE_STMT` indicates if this is a UPDATE statement or a DELETE statement.
pub const FLAG_IN_UPDATE_OR_DELETE_STMT: u64 = 1 << 4;
/// `FLAG_IN_SELECT_STMT` indicates if this is a SELECT statement.
pub const FLAG_IN_SELECT_STMT: u64 = 1 << 5;
/// `FLAG_OVERFLOW_AS_WARNING` indicates if overflow error should be returned as warning.
/// In strict sql mode, overflow error should be returned as error,
/// in non-strict sql mode, overflow error should be saved as warning.
pub const FLAG_OVERFLOW_AS_WARNING: u64 = 1 << 6;

// FLAG_DIVIDED_BY_ZERO_AS_WARNING indicates if DividedByZero should be returned as warning.
pub const FLAG_DIVIDED_BY_ZERO_AS_WARNING: u64 = 1 << 8;

pub const MODE_NO_ZERO_DATE_MODE: u64 = 25;
pub const MODE_ERROR_FOR_DIVISION_BY_ZERO: u64 = 27;

const DEFAULT_MAX_WARNING_CNT: usize = 64;

#[derive(Clone, Debug)]
pub struct EvalConfig {
    /// timezone to use when parse/calculate time.
    pub tz: Tz,
    pub ignore_truncate: bool,
    pub truncate_as_warning: bool,
    pub overflow_as_warning: bool,
    pub in_insert_stmt: bool,
    pub in_update_or_delete_stmt: bool,
    pub in_select_stmt: bool,
    pub pad_char_to_full_length: bool,
    pub divided_by_zero_as_warning: bool,
    // TODO: max warning count is not really a EvalConfig. Instead it is a ExecutionConfig, because
    // warning is a executor stuff instead of a evaluation stuff.
    pub max_warning_cnt: usize,
    pub sql_mode: u64,
    /// if the session is in strict mode.
    pub strict_sql_mode: bool,
}

impl Default for EvalConfig {
    fn default() -> EvalConfig {
        EvalConfig::new()
    }
}

impl EvalConfig {
    pub fn new() -> Self {
        Self {
            tz: Tz::utc(),
            ignore_truncate: false,
            truncate_as_warning: false,
            overflow_as_warning: false,
            in_insert_stmt: false,
            in_update_or_delete_stmt: false,
            in_select_stmt: false,
            pad_char_to_full_length: false,
            divided_by_zero_as_warning: false,
            max_warning_cnt: DEFAULT_MAX_WARNING_CNT,
            sql_mode: 0,
            strict_sql_mode: false,
        }
    }

    pub fn from_flags(flags: u64) -> Self {
        let mut config = Self::new();
        config.set_by_flags(flags);
        config
    }

    pub fn set_ignore_truncate(&mut self, new_value: bool) -> &mut Self {
        self.ignore_truncate = new_value;
        self
    }

    pub fn set_truncate_as_warning(&mut self, new_value: bool) -> &mut Self {
        self.truncate_as_warning = new_value;
        self
    }

    pub fn set_overflow_as_warning(&mut self, new_value: bool) -> &mut Self {
        self.overflow_as_warning = new_value;
        self
    }

    pub fn set_in_insert_stmt(&mut self, new_value: bool) -> &mut Self {
        self.in_insert_stmt = new_value;
        self
    }

    pub fn set_in_update_or_delete_stmt(&mut self, new_value: bool) -> &mut Self {
        self.in_update_or_delete_stmt = new_value;
        self
    }

    pub fn set_in_select_stmt(&mut self, new_value: bool) -> &mut Self {
        self.in_select_stmt = new_value;
        self
    }

    pub fn set_pad_char_to_full_length(&mut self, new_value: bool) -> &mut Self {
        self.pad_char_to_full_length = new_value;
        self
    }

    pub fn set_divided_by_zero_as_warning(&mut self, new_value: bool) -> &mut Self {
        self.divided_by_zero_as_warning = new_value;
        self
    }

    pub fn set_max_warning_cnt(&mut self, new_value: usize) -> &mut Self {
        self.max_warning_cnt = new_value;
        self
    }

    pub fn set_sql_mode(&mut self, new_value: u64) -> &mut Self {
        self.sql_mode = new_value;
        self
    }

    pub fn set_strict_sql_mode(&mut self, new_value: bool) -> &mut Self {
        self.strict_sql_mode = new_value;
        self
    }

    pub fn set_time_zone_by_name(&mut self, tz_name: &str) -> Result<&mut Self> {
        match Tz::from_tz_name(tz_name) {
            Some(tz) => {
                self.tz = tz;
                Ok(self)
            }
            None => Err(Error::invalid_timezone(tz_name)),
        }
    }

    pub fn set_time_zone_by_offset(&mut self, offset_sec: i64) -> Result<&mut Self> {
        match Tz::from_offset(offset_sec) {
            Some(tz) => {
                self.tz = tz;
                Ok(self)
            }
            None => Err(Error::invalid_timezone(&format!("offset {}s", offset_sec))),
        }
    }

    pub fn set_by_flags(&mut self, flags: u64) -> &mut Self {
        self.set_ignore_truncate((flags & FLAG_IGNORE_TRUNCATE) > 0)
            .set_truncate_as_warning((flags & FLAG_TRUNCATE_AS_WARNING) > 0)
            .set_overflow_as_warning((flags & FLAG_OVERFLOW_AS_WARNING) > 0)
            .set_in_insert_stmt((flags & FLAG_IN_INSERT_STMT) > 0)
            .set_in_update_or_delete_stmt((flags & FLAG_IN_UPDATE_OR_DELETE_STMT) > 0)
            .set_in_select_stmt((flags & FLAG_IN_SELECT_STMT) > 0)
            .set_pad_char_to_full_length((flags & FLAG_PAD_CHAR_TO_FULL_LENGTH) > 0)
            .set_divided_by_zero_as_warning((flags & FLAG_DIVIDED_BY_ZERO_AS_WARNING) > 0)
    }

    /// detects if 'ERROR_FOR_DIVISION_BY_ZERO' mode is set in sql_mode
    pub fn mode_error_for_division_by_zero(&self) -> bool {
        self.sql_mode & MODE_ERROR_FOR_DIVISION_BY_ZERO == MODE_ERROR_FOR_DIVISION_BY_ZERO
    }

    /// detects if 'MODE_NO_ZERO_DATE_MODE' mode is set in sql_mode
    pub fn mode_no_zero_date_mode(&self) -> bool {
        self.sql_mode & MODE_NO_ZERO_DATE_MODE == MODE_NO_ZERO_DATE_MODE
    }

    pub fn new_eval_warnings(&self) -> EvalWarnings {
        EvalWarnings::new(self.max_warning_cnt)
    }

    #[cfg(test)]
    pub fn default_for_test() -> EvalConfig {
        let mut config = EvalConfig::new();
        config.set_ignore_truncate(true);
        config
    }
}

// Warning details caused in eval computation.
#[derive(Debug)]
pub struct EvalWarnings {
    // max number of warnings to return.
    max_warning_cnt: usize,
    // number of warnings
    pub warning_cnt: usize,
    // details of previous max_warning_cnt warnings
    pub warnings: Vec<select::Error>,
}

impl EvalWarnings {
    pub fn new(max_warning_cnt: usize) -> EvalWarnings {
        EvalWarnings {
            max_warning_cnt,
            warning_cnt: 0,
            warnings: Vec::with_capacity(max_warning_cnt),
        }
    }

    pub fn append_warning(&mut self, err: Error) {
        self.warning_cnt += 1;
        if self.warnings.len() < self.max_warning_cnt {
            self.warnings.push(err.into());
        }
    }

    pub fn merge(&mut self, other: &mut EvalWarnings) {
        self.warning_cnt += other.warning_cnt;
        if self.warnings.len() >= self.max_warning_cnt {
            return;
        }
        other
            .warnings
            .truncate(self.max_warning_cnt - self.warnings.len());
        self.warnings.append(&mut other.warnings);
    }
}

#[derive(Debug)]
/// Some global variables needed in an evaluation.
pub struct EvalContext {
    pub cfg: Arc<EvalConfig>,
    pub warnings: EvalWarnings,
}

impl Default for EvalContext {
    fn default() -> EvalContext {
        let cfg = Arc::new(EvalConfig::default());
        let warnings = cfg.new_eval_warnings();
        EvalContext { cfg, warnings }
    }
}

impl EvalContext {
    pub fn new(cfg: Arc<EvalConfig>) -> EvalContext {
        let warnings = cfg.new_eval_warnings();
        EvalContext { cfg, warnings }
    }

    pub fn handle_truncate(&mut self, is_truncated: bool) -> Result<()> {
        if !is_truncated {
            return Ok(());
        }
        self.handle_truncate_err(Error::truncated())
    }

    pub fn handle_truncate_err(&mut self, err: Error) -> Result<()> {
        if self.cfg.ignore_truncate {
            return Ok(());
        }
        if self.cfg.truncate_as_warning {
            self.warnings.append_warning(err);
            return Ok(());
        }
        Err(err)
    }

    /// handle_overflow treats ErrOverflow as warnings or returns the error
    /// based on the cfg.handle_overflow state.
    pub fn handle_overflow(&mut self, err: Error) -> Result<()> {
        if self.cfg.overflow_as_warning {
            self.warnings.append_warning(err);
            Ok(())
        } else {
            Err(err)
        }
    }

    pub fn handle_division_by_zero(&mut self) -> Result<()> {
        if self.cfg.in_insert_stmt || self.cfg.in_update_or_delete_stmt {
            if !self.cfg.mode_error_for_division_by_zero() {
                return Ok(());
            }
            if self.cfg.strict_sql_mode && !self.cfg.divided_by_zero_as_warning {
                return Err(Error::division_by_zero());
            }
        }
        self.warnings.append_warning(Error::division_by_zero());
        Ok(())
    }

    pub fn handle_invalid_time_error(&mut self, err: Error) -> Result<()> {
        if err.code() != super::codec::error::ERR_TRUNCATE_WRONG_VALUE {
            return Err(err);
        }
        let cfg = &self.cfg;
        if cfg.strict_sql_mode && (cfg.in_insert_stmt || cfg.in_update_or_delete_stmt) {
            Err(err)
        } else {
            self.warnings.append_warning(err);
            Ok(())
        }
    }

    pub fn overflow_from_cast_str_as_int(
        &mut self,
        bytes: &[u8],
        orig_err: Error,
        negative: bool,
    ) -> Result<i64> {
        if !self.cfg.in_select_stmt || !self.cfg.overflow_as_warning {
            return Err(orig_err);
        }
        let orig_str = String::from_utf8_lossy(bytes);
        self.warnings
            .append_warning(Error::truncated_wrong_val("INTEGER", &orig_str));
        if negative {
            Ok(i64::MIN)
        } else {
            Ok(u64::MAX as i64)
        }
    }

    pub fn take_warnings(&mut self) -> EvalWarnings {
        mem::replace(
            &mut self.warnings,
            EvalWarnings::new(self.cfg.max_warning_cnt),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::super::Error;
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_handle_truncate() {
        // ignore_truncate = false, truncate_as_warning = false
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new()));
        assert!(ctx.handle_truncate(false).is_ok());
        assert!(ctx.handle_truncate(true).is_err());
        assert!(ctx.take_warnings().warnings.is_empty());
        // ignore_truncate = false;
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        assert!(ctx.handle_truncate(false).is_ok());
        assert!(ctx.handle_truncate(true).is_ok());
        assert!(ctx.take_warnings().warnings.is_empty());

        // ignore_truncate = false, truncate_as_warning = true
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::from_flags(FLAG_TRUNCATE_AS_WARNING)));
        assert!(ctx.handle_truncate(false).is_ok());
        assert!(ctx.handle_truncate(true).is_ok());
        assert!(!ctx.take_warnings().warnings.is_empty());
    }

    #[test]
    fn test_max_warning_cnt() {
        let eval_cfg = Arc::new(EvalConfig::from_flags(FLAG_TRUNCATE_AS_WARNING));
        let mut ctx = EvalContext::new(Arc::clone(&eval_cfg));
        assert!(ctx.handle_truncate(true).is_ok());
        assert!(ctx.handle_truncate(true).is_ok());
        assert_eq!(ctx.take_warnings().warnings.len(), 2);
        for _ in 0..2 * DEFAULT_MAX_WARNING_CNT {
            assert!(ctx.handle_truncate(true).is_ok());
        }
        let warnings = ctx.take_warnings();
        assert_eq!(warnings.warning_cnt, 2 * DEFAULT_MAX_WARNING_CNT);
        assert_eq!(warnings.warnings.len(), eval_cfg.max_warning_cnt);
    }

    #[test]
    fn test_handle_division_by_zero() {
        let cases = vec![
            //(flag,sql_mode,strict_sql_mode,is_ok,is_empty)
            (0, 0, false, true, false), //warning
            (
                FLAG_IN_INSERT_STMT,
                MODE_ERROR_FOR_DIVISION_BY_ZERO,
                false,
                true,
                false,
            ), //warning
            (
                FLAG_IN_UPDATE_OR_DELETE_STMT,
                MODE_ERROR_FOR_DIVISION_BY_ZERO,
                false,
                true,
                false,
            ), //warning
            (
                FLAG_IN_UPDATE_OR_DELETE_STMT,
                MODE_ERROR_FOR_DIVISION_BY_ZERO,
                true,
                false,
                true,
            ), //error
            (FLAG_IN_UPDATE_OR_DELETE_STMT, 0, true, true, true), //ok
            (
                FLAG_IN_UPDATE_OR_DELETE_STMT | FLAG_DIVIDED_BY_ZERO_AS_WARNING,
                MODE_ERROR_FOR_DIVISION_BY_ZERO,
                true,
                true,
                false,
            ), //warning
        ];
        for (flag, sql_mode, strict_sql_mode, is_ok, is_empty) in cases {
            let mut cfg = EvalConfig::new();
            cfg.set_by_flags(flag)
                .set_sql_mode(sql_mode)
                .set_strict_sql_mode(strict_sql_mode);
            let mut ctx = EvalContext::new(Arc::new(cfg));
            assert_eq!(ctx.handle_division_by_zero().is_ok(), is_ok);
            assert_eq!(ctx.take_warnings().warnings.is_empty(), is_empty);
        }
    }

    #[test]
    fn test_handle_invalid_time_error() {
        let cases = vec![
            //(flags,strict_sql_mode,is_ok,is_empty)
            (0, false, true, false),                             //warning
            (0, true, true, false),                              //warning
            (FLAG_IN_INSERT_STMT, false, true, false),           //warning
            (FLAG_IN_UPDATE_OR_DELETE_STMT, false, true, false), //warning
            (FLAG_IN_UPDATE_OR_DELETE_STMT, true, false, true),  //error
            (FLAG_IN_INSERT_STMT, true, false, true),            //error
        ];
        for (flags, strict_sql_mode, is_ok, is_empty) in cases {
            let err = Error::invalid_time_format("");
            let mut cfg = EvalConfig::new();
            cfg.set_by_flags(flags).set_strict_sql_mode(strict_sql_mode);
            let mut ctx = EvalContext::new(Arc::new(cfg));
            assert_eq!(ctx.handle_invalid_time_error(err).is_ok(), is_ok);
            assert_eq!(ctx.take_warnings().warnings.is_empty(), is_empty);
        }
    }
}

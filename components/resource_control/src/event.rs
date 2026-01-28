// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    fmt::{Display, Formatter},
};

use crate::{Config, Resource, Usages};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Scope {
    Global,
    ResourceGroup { name: String },
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Severity {
    #[default]
    Normal,
    Stressed,
    Critical,
    Exhausted,
}

#[derive(Debug, Clone, Copy)]
pub struct SeverityThreshold {
    pub stressed: f64,
    pub critical: f64,
    pub exhausted: f64,
}

impl Default for SeverityThreshold {
    fn default() -> SeverityThreshold {
        SeverityThreshold {
            stressed: 0.4,
            critical: 0.7,
            exhausted: 0.9,
        }
    }
}

impl Severity {
    pub(crate) fn get_severity(ratio: f64, severity_threshold: SeverityThreshold) -> Severity {
        let mut severity = Severity::Normal;
        if ratio >= severity_threshold.exhausted {
            severity = Severity::Exhausted;
        } else if ratio >= severity_threshold.critical {
            severity = Severity::Critical;
        } else if ratio >= severity_threshold.stressed {
            severity = Severity::Stressed;
        }
        severity
    }

    pub(crate) fn is_abnormal(&self) -> bool {
        !self.eq(&Severity::Normal)
    }
}

impl Display for Severity {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Normal => {
                write!(f, "Normal")?;
            }
            Severity::Stressed => {
                write!(f, "Stressed")?;
            }
            Severity::Critical => {
                write!(f, "Critical")?;
            }
            Severity::Exhausted => {
                write!(f, "Exhausted")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Metric {
    pub resource: Resource,
    pub scope: Scope,
    pub severity: Severity,
}

pub enum ResourceEvent {
    CollectMetric(Metric),
    DispatchUsages(Usages),
    UpdateConfig(Config),
    Tick,
    Stop,
}

impl Display for ResourceEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ResourceEvent::CollectMetric { .. } => {
                write!(f, "CollectMetric")?;
            }
            ResourceEvent::DispatchUsages { .. } => {
                write!(f, "DispatchUsages")?;
            }
            ResourceEvent::UpdateConfig(_) => {
                write!(f, "UpdateConfig")?;
            }
            ResourceEvent::Tick => {
                write!(f, "Tick")?;
            }
            ResourceEvent::Stop => {
                write!(f, "Stop")?;
            }
        }
        Ok(())
    }
}

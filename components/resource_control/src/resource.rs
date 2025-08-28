// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::error;

#[derive(Debug, Clone, Copy)]
pub enum ResourceValue {
    Num { value: u64 },
    CpuTime { value: f64 },
    Bytes { value: u64 },
}

impl ResourceValue {
    pub(crate) fn cpu_time(&self) -> f64 {
        match self {
            ResourceValue::CpuTime { value } => *value,
            _ => 0.0,
        }
    }

    pub(crate) fn bytes(&self) -> u64 {
        match self {
            ResourceValue::Bytes { value } => *value,
            _ => 0,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn num(&self) -> u64 {
        match self {
            ResourceValue::Num { value } => *value,
            _ => 0,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn add(&self, rhs: &Self) -> Self {
        match self {
            ResourceValue::Num { value } => {
                let mut new_value = *value;
                if let ResourceValue::Num { value: rhs_value } = rhs {
                    new_value += *rhs_value;
                }
                ResourceValue::Num { value: new_value }
            }
            ResourceValue::CpuTime { value } => {
                let mut new_value = *value;
                if let ResourceValue::CpuTime { value: rhs_value } = rhs {
                    new_value += *rhs_value;
                }
                ResourceValue::CpuTime { value: new_value }
            }
            ResourceValue::Bytes { value } => {
                let mut new_value = *value;
                if let ResourceValue::Bytes { value: rhs_value } = rhs {
                    new_value += *rhs_value;
                }
                ResourceValue::Bytes { value: new_value }
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn subtract(&self, rhs: &Self) -> Self {
        match self {
            ResourceValue::Num { value } => {
                let mut new_value = *value;
                if let ResourceValue::Num { value: rhs_value } = rhs {
                    new_value -= *rhs_value;
                }
                ResourceValue::Num { value: new_value }
            }
            ResourceValue::CpuTime { value } => {
                let mut new_value = *value;
                if let ResourceValue::CpuTime { value: rhs_value } = rhs {
                    new_value -= *rhs_value;
                }
                ResourceValue::CpuTime { value: new_value }
            }
            ResourceValue::Bytes { value } => {
                let mut new_value = *value;
                if let ResourceValue::Bytes { value: rhs_value } = rhs {
                    new_value -= *rhs_value;
                }
                ResourceValue::Bytes { value: new_value }
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn multiply(&self, rhs: &Self) -> Self {
        match self {
            ResourceValue::Num { value } => {
                let mut new_value = *value;
                if let ResourceValue::Num { value: rhs_value } = rhs {
                    new_value *= *rhs_value;
                }
                ResourceValue::Num { value: new_value }
            }
            ResourceValue::CpuTime { value } => {
                let mut new_value = *value;
                if let ResourceValue::CpuTime { value: rhs_value } = rhs {
                    new_value *= *rhs_value;
                }
                ResourceValue::CpuTime { value: new_value }
            }
            ResourceValue::Bytes { value } => {
                let mut new_value = *value;
                if let ResourceValue::Bytes { value: rhs_value } = rhs {
                    new_value *= *rhs_value;
                }
                ResourceValue::Bytes { value: new_value }
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn divide(&self, rhs: &Self) -> Self {
        match self {
            ResourceValue::Num { value } => {
                let mut new_value = *value;
                if let ResourceValue::Num { value: rhs_value } = rhs {
                    if *rhs_value != 0 {
                        new_value /= *rhs_value;
                    } else {
                        error!(
                            "resource value Num {}/{} err, division by zero",
                            new_value, *rhs_value
                        )
                    }
                }
                ResourceValue::Num { value: new_value }
            }
            ResourceValue::CpuTime { value } => {
                let mut new_value = *value;
                if let ResourceValue::CpuTime { value: rhs_value } = rhs {
                    if *rhs_value != 0.0 {
                        new_value /= *rhs_value;
                    } else {
                        error!(
                            "resource value CpuTime {}/{} err, division by zero",
                            new_value, *rhs_value
                        )
                    }
                }
                ResourceValue::CpuTime { value: new_value }
            }
            ResourceValue::Bytes { value } => {
                let mut new_value = *value;
                if let ResourceValue::Bytes { value: rhs_value } = rhs {
                    if *rhs_value != 0 {
                        new_value /= *rhs_value;
                    } else {
                        error!(
                            "resource value Bytes {}/{} err, division by zero",
                            new_value, *rhs_value
                        )
                    }
                }
                ResourceValue::Bytes { value: new_value }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Usage {
    pub resource_type: ResourceType,
    pub resource_value: ResourceValue,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceType {
    Cpu {
        cpu_type: CpuType,
    },
    Memory,
    Disk,
    Network,
    Read,
    Write,
    PendingCompactionWal,
    MemTable,
    L0Table,
    #[default]
    Unknown,
}

impl ResourceType {
    #[allow(dead_code)]
    pub(crate) fn is_unknown(&self) -> bool {
        self.eq(&ResourceType::Unknown)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Resource {
    // Physical
    Cpu { cpu_type: CpuType, cpu_time: f64 },
    Memory { used_bytes: u64 },
    Disk { used_bytes: u64 },
    Network { used_bytes: u64 },
    // Logical
    Read { bytes: u64 },
    Write { bytes: u64 },
    PendingCompactionWal { count: u64 },
    MemTable { bytes: u64 },
    L0Table { bytes: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CpuType {
    ToTal,
    RaftWorker,
    RaftIo,
    Apply,
    StatusServer,
    PdWorker,
    GrpcServer,
    UnifiedReadPool,
    TxnScheduler,
}

impl Resource {
    #[allow(dead_code)]
    pub(crate) fn resource_type(&self) -> ResourceType {
        let resource_usage = self.resource_usage();
        resource_usage.resource_type
    }

    pub(crate) fn resource_usage(&self) -> Usage {
        match self {
            Resource::Cpu { cpu_type, cpu_time } => Usage {
                resource_type: ResourceType::Cpu {
                    cpu_type: *cpu_type,
                },
                resource_value: ResourceValue::CpuTime { value: *cpu_time },
            },
            Resource::Memory { used_bytes } => Usage {
                resource_type: ResourceType::Memory,
                resource_value: ResourceValue::Bytes { value: *used_bytes },
            },
            Resource::Disk { used_bytes } => Usage {
                resource_type: ResourceType::Disk,
                resource_value: ResourceValue::Bytes { value: *used_bytes },
            },
            Resource::Network { used_bytes } => Usage {
                resource_type: ResourceType::Network,
                resource_value: ResourceValue::Bytes { value: *used_bytes },
            },
            Resource::Read { bytes } => Usage {
                resource_type: ResourceType::Read,
                resource_value: ResourceValue::Bytes { value: *bytes },
            },
            Resource::Write { bytes } => Usage {
                resource_type: ResourceType::Write,
                resource_value: ResourceValue::Bytes { value: *bytes },
            },
            Resource::PendingCompactionWal { count } => Usage {
                resource_type: ResourceType::PendingCompactionWal,
                resource_value: ResourceValue::Num { value: *count },
            },
            Resource::MemTable { bytes } => Usage {
                resource_type: ResourceType::MemTable,
                resource_value: ResourceValue::Bytes { value: *bytes },
            },
            Resource::L0Table { bytes } => Usage {
                resource_type: ResourceType::L0Table,
                resource_value: ResourceValue::Bytes { value: *bytes },
            },
        }
    }
}

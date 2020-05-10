#[repr(u32)]
pub enum Trace {
    #[allow(dead_code)]
    Unknown = 0u32,
    Copr,
}

impl Into<u32> for Trace {
    fn into(self) -> u32 {
        self as u32
    }
}

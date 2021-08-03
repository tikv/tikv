use crate::NUM_CFS;


pub struct Options {
    pub cfs: [CFConfig; NUM_CFS],
}

#[derive(Default, Clone, Copy)]
pub struct CFConfig {
    
}
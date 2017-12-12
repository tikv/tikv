use libc;

pub enum AdjustPriType {
    Incr,
    Normal,
    Desc,
}

pub fn adjust_priority(t: AdjustPriType) {
    if cfg!(target_os = "unix") {
        unsafe {
            let pid = libc::pthread_self();
            let pri = match t {
                AdjustPriType::Incr => -1,
                AdjustPriType::Normal => 0,
                AdjustPriType::Desc => 1,
            };
            libc::setpriority(libc::PRIO_PROCESS as u32, pid as u32, pri);
        }
    }
}

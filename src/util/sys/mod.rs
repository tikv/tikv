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
                Incr => -1,
                Normal => 0,
                Desc => 1,
            };
            libc::setpriority(libc::PRIO_PROCESS, pid, pri);
        }
    }
}

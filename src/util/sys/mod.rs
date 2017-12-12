#[cfg(target_os = "linux")]
use libc;

pub enum AdjustPriType {
    Incr,
    Normal,
    Desc,
}

#[cfg(target_os = "linux")]
pub fn adjust_priority(t: AdjustPriType) {
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

#[cfg(not(target_os = "linux"))]
pub fn adjust_priority(_: AdjustPriType) {}

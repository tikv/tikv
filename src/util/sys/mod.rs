#[cfg(target_os = "linux")]
use libc;
#[cfg(target_os = "linux")]
use std::io::Error;

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
        if libc::setpriority(libc::PRIO_PROCESS as u32, pid as u32, pri) != 0 {
            warn!(
                "set thread priority to {} failed, error {:?}",
                pri,
                Error::last_os_err()
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub fn adjust_priority(_: AdjustPriType) {}

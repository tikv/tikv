use engine_traits::{CF_DEFAULT, CF_WRITE};
use kvproto::raft_cmdpb::{RaftCmdRequest, Request, CmdType};
use txn_types::Key;

pub fn format_raft_cmd(cmd: &RaftCmdRequest) -> String {
    let info = cmd.get_requests().iter().map(fmt_req).collect::<Vec<String>>().join(",");
    if let Some(ts) = extract_ts(cmd) {
        format!("[{}]@{}", info, ts)
    } else {
        format!("[{}]", info)
    }
}

pub fn extract_ts(cmd: &RaftCmdRequest) -> Option<u64> {
    let flag_data = cmd.get_header().get_flag_data();
    if flag_data.len() == 8 {
        Some(u64::from_be_bytes(flag_data.try_into().unwrap()))
    } else {
        None
    }
}

fn fmt_req(req: &Request) -> String {
    let cmd_type = req.get_cmd_type();
    match cmd_type {
        CmdType::Get => format!("{:?}{}", cmd_type, fmt_cf_key(req.get_get().get_cf(), req.get_get().get_key())),
        CmdType::Put => format!("{:?}{}", cmd_type, fmt_cf_key(req.get_put().get_cf(), req.get_put().get_key())),
        CmdType::Delete => format!("{:?}{}", cmd_type, fmt_cf_key(req.get_delete().get_cf(), req.get_delete().get_key())),
        CmdType::Prewrite => format!("{:?}({},{})", cmd_type, log_wrappers::hex_encode(req.get_prewrite().get_key()), log_wrappers::hex_encode(req.get_prewrite().get_lock())),
        CmdType::ReadIndex => format!("{:?}({},[{}])", cmd_type, req.get_read_index().get_start_ts(), req.get_read_index().get_key_ranges().iter().map(|r| format!("{}:{}", log_wrappers::hex_encode(r.get_start_key()), log_wrappers::hex_encode(r.get_end_key()))).collect::<Vec<String>>().join(",")),
        _ => format!("{:?}", cmd_type),
    }
}

fn fmt_cf_key(cf: &str, key: &[u8]) -> String {
    let cf = if cf.is_empty() { CF_DEFAULT } else { cf };
    let key_ts = if cf == CF_DEFAULT || cf == CF_WRITE {
        Key::split_on_ts_for(key).ok()
    } else {
        None
    };
    if let Some((k, ts)) = key_ts {
        format!("({},{}@{})", cf, log_wrappers::hex_encode(k), ts)
    } else {
        format!("({},{})", cf, log_wrappers::hex_encode(key))
    }
}



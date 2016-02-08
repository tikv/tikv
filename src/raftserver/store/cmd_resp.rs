use proto::raft_cmdpb::RaftCommandResponse;
use proto::errorpb::ErrorDetail;

pub fn region_not_found_error(region_id: u64) -> RaftCommandResponse {
    let mut detail = ErrorDetail::new();
    {
        let mut msg = detail.mut_region_not_found();
        msg.set_region_id(region_id);
    }
    detail_error("region is not found", detail)
}

pub fn message_error<T: Into<String>>(message: T) -> RaftCommandResponse {
    new_error(message, None)
}

pub fn detail_error<T: Into<String>>(message: T, detail: ErrorDetail) -> RaftCommandResponse {
    new_error(message, Some(detail))
}

fn new_error<T>(message: T, detail: Option<ErrorDetail>) -> RaftCommandResponse
    where T: Into<String>
{
    let mut msg = RaftCommandResponse::new();
    {
        let mut error = msg.mut_header().mut_error();
        error.set_message(message.into());
        if let Some(detail) = detail {
            error.set_detail(detail)
        }
    }

    msg
}

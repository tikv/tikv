use std::sync::atomic::{AtomicUsize, Ordering};

use protobuf::RepeatedField;

use kvproto::pdpb::{Member, GetMembersRequest, GetMembersResponse, ResponseHeader};

use super::Case;
use super::Result;

#[derive(Debug)]
pub struct Split {
    resps: Vec<GetMembersResponse>,
    idx: AtomicUsize,
}

impl Split {
    pub fn new(eps: Vec<String>) -> Split {
        let mut members = Vec::with_capacity(eps.len());
        for (i, ep) in (&eps).into_iter().enumerate() {
            let mut m = Member::new();
            m.set_name(format!("pd{}", i));
            m.set_member_id(100 + i as u64);
            m.set_client_urls(RepeatedField::from_vec(vec![ep.to_owned()]));
            m.set_peer_urls(RepeatedField::from_vec(vec![ep.to_owned()]));
            members.push(m);
        }

        let mut resps = Vec::with_capacity(eps.len());
        for (i, _) in (&eps).into_iter().enumerate() {
            let mut resp = GetMembersResponse::new();
            let mut header = ResponseHeader::new();
            header.set_cluster_id(i as u64 + 1); // start from 1.
            resp.set_header(header.clone());
            resp.set_members(RepeatedField::from_vec(members.clone()));
            resp.set_leader(members[0].clone());
            resps.push(resp);
        }

        info!("[Split] resps {:?}", resps);

        Split {
            resps: resps,
            idx: AtomicUsize::new(0),
        }
    }
}

impl Case for Split {
    fn GetMembers(&self, _: &GetMembersRequest) -> Option<Result<GetMembersResponse>> {
        let idx = self.idx.fetch_add(1, Ordering::SeqCst);
        info!("[Split] GetMembers: {:?}",
              self.resps[idx % self.resps.len()]);
        Some(Ok(self.resps[idx % self.resps.len()].clone()))
    }
}

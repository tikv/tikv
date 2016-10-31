
extern crate getopts;
extern crate uuid;
extern crate kvproto;
extern crate tikv;

use std::env;
use std::net::SocketAddr;

use uuid::Uuid;

use getopts::{Matches, Options};
use kvproto::metapb::Peer;
use kvproto::raft_cmdpb::{RaftCmdRequest, StatusRequest, StatusCmdType};
use tikv::util::client::{Client, TikvClient};

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn get_opt(matches: &Matches, short: &str, default: &str) -> String {
    if matches.opt_present(short) {
        matches.opt_str(short).unwrap()
    } else {
        default.to_owned()
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    opts.optopt("a",
                "address",
                "set address",
                "if not set, use localhost:30161.");
    opts.optopt("c", "cmd", "cmd type", "default: status cmd");
    opts.optflag("h", "help", "print help");
    opts.optopt("r", "region", "region id", "default: 1");
    opts.optopt("p", "peer", "peer id", "default: 1");
    opts.optopt("s", "store", "store id", "default: 1");

    let matches = opts.parse(&args[1..]).expect("opts parse failed");
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let address = get_opt(&matches, "a", "localhost:30161");
    // let cmd = get_opt(&matches, "c", "status");
    let region_id: u64 = get_opt(&matches, "r", "1").parse().expect("invalid region id");
    let peer_id: u64 = get_opt(&matches, "p", "1").parse().expect("invalid peer id");
    let store_id: u64 = get_opt(&matches, "s", "1").parse().expect("invalid store id");

    let addr: SocketAddr = address.parse().expect("ip error");
    let c = TikvClient::new(addr);

    let mut peer = Peer::new();
    peer.set_id(peer_id);
    peer.set_store_id(store_id);

    let mut status = StatusRequest::new();
    status.set_cmd_type(StatusCmdType::RegionDetail);

    let mut req = RaftCmdRequest::new();
    req.mut_header().set_region_id(region_id);
    req.mut_header().set_peer(peer);
    req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());

    req.set_status_request(status);

    match c.send_cmd(&req) {
        Ok(resp) => {
            print!("get resp {:?}", resp);
        }
        Err(e) => {
            print!("get error {:?}", e);
        }
    }
}

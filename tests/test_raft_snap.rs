use test_raft::*;
use kvproto::raftpb::*;
use tikv::raft::*;

fn testing_snap() -> Snapshot {
    new_snapshot(11, 11, vec![1, 2])
}


#[test]
fn test_sending_snapshot_set_pending_snapshot() {
    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
    sm.restore(testing_snap());

    sm.become_candidate();
    sm.become_leader();

    // force set the next of node 1, so that
    // node 1 needs a snapshot
    sm.prs.get_mut(&2).unwrap().next_idx = sm.raft_log.first_index();

    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.set_index(sm.prs[&2].next_idx - 1);
    m.set_reject(true);
    sm.step(m).expect("");
    assert_eq!(sm.prs[&2].pending_snapshot, 11);
}

#[test]
fn test_pending_snapshot_pause_replication() {
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    sm.restore(testing_snap());

    sm.become_candidate();
    sm.become_leader();

    sm.prs.get_mut(&2).unwrap().become_snapshot(11);

    sm.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");
    let msgs = sm.read_messages();
    assert!(msgs.is_empty());
}

#[test]
fn test_snapshot_failure() {
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    sm.restore(testing_snap());

    sm.become_candidate();
    sm.become_leader();

    sm.prs.get_mut(&2).unwrap().next_idx = 1;
    sm.prs.get_mut(&2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    m.set_reject(true);
    sm.step(m).expect("");
    assert_eq!(sm.prs[&2].pending_snapshot, 0);
    assert_eq!(sm.prs[&2].next_idx, 1);
    assert!(sm.prs[&2].paused);
}

#[test]
fn test_snapshot_succeed() {
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    sm.restore(testing_snap());

    sm.become_candidate();
    sm.become_leader();

    sm.prs.get_mut(&2).unwrap().next_idx = 1;
    sm.prs.get_mut(&2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgSnapStatus, 0);
    m.set_reject(false);
    sm.step(m).expect("");
    assert_eq!(sm.prs[&2].pending_snapshot, 0);
    assert_eq!(sm.prs[&2].next_idx, 12);
    assert!(sm.prs[&2].paused);
}

#[test]
fn test_snapshot_abort() {
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    sm.restore(testing_snap());

    sm.become_candidate();
    sm.become_leader();

    sm.prs.get_mut(&2).unwrap().next_idx = 1;
    sm.prs.get_mut(&2).unwrap().become_snapshot(11);

    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.set_index(11);
    // A successful MsgAppendResponse that has a higher/equal index than the
    // pending snapshot should abort the pending snapshot.
    sm.step(m).expect("");
    assert_eq!(sm.prs[&2].pending_snapshot, 0);
    assert_eq!(sm.prs[&2].next_idx, 12);
}

# Raftstore Maintenance Notes

## Critical Invariants

- Peer destruction can wait for persisted readies and flushed log GC before
  scheduling metadata cleanup. Once scheduled, cleanup and destruction observers
  must not run again, and late merge messages must preserve `ReadyToDestroy`
  while waiting for the region worker's completion message.
  Completion identifies the original FSM instance as well as its peer ID,
  because split may replace an uninitialized FSM with the same peer ID. Under
  the `StoreMeta` lock, finalization must leave a split replacement's mailbox
  and shared metadata intact even if the old FSM already received completion.
  A late target snapshot must wait for the source's existing cleanup. Cleanup
  completion reconciles its dependency under the `StoreMeta` lock even if it
  precedes the snapshot's Step1 notification. An independently destroyed source
  can release a `need_atomic` dependency only while its readiness flag is false;
  a ready source must finish through the atomic snapshot protocol.

## Log Volume And Observability

- Extra-message transport `Full` warnings are limited per poller, target store,
  and message type. The next emitted warning reports the suppressed count;
  `tikv_raftstore_extra_message_send_failure_total` counts every failure,
  including suppressed ones. Other transport failures remain at WARN.
- Network-delivered Raft local messages (`StepLocalMsg`) are logged at DEBUG
  and counted by the `step_local_msg` message-drop reason. Other Raft handling
  errors remain at WARN.
- Snapshot precheck responses use sampled INFO logs and DEBUG for the remaining
  responses. Use `tikv_raftstore_snap_gen_precheck_response_total` to compare
  passed, rejected, and ignored outcomes; log samples are not a count of events.
  Sampling must not change snapshot scheduling or precheck decisions.

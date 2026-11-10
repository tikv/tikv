# Raftstore Peer Destruction

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

# Raftstore Peer Destruction

## Critical Invariants

- Peer destruction can wait for persisted readies and flushed log GC before
  scheduling metadata cleanup. Once scheduled, cleanup and destruction observers
  must not run again, and late merge messages must preserve `ReadyToDestroy`
  until the region worker's completion message stops the peer FSM.
  A late target snapshot must wait for the source's existing cleanup. Cleanup
  completion reconciles its dependency under the `StoreMeta` lock even if it
  precedes the snapshot's Step1 notification. An independently destroyed source
  can release a `need_atomic` dependency only while its readiness flag is false;
  a ready source must finish through the atomic snapshot protocol.

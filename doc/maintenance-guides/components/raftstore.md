# Classic Raftstore Load-Based Splits

## Purpose And Scope

This guide covers the classic raftstore load-based auto-split path. It is not a
complete guide to raftstore or to the separate raftstore-v2 implementation.

## Architecture And Lifecycle

The load reporter schedules candidates on the PD worker. The worker refreshes
the Region through PD and routes a split-key candidate to the local peer FSM.
The FSM validates the request and schedules a split-ID request on the PD
worker. A stopped or full scheduler returns an error through the callback.

## Metadata Contracts And Invariants

- A candidate's sampled peer may be stale. The local peer FSM must build the
  split request from its current Region and peer, and a local follower must
  reject the candidate rather than forward it to a remote leader.
- The peer FSM checks the Region version and requires an auto-split key to be
  strictly inside the current Region's range before requesting split IDs.
- Normal load-based splits use `SplitReason::Load`, derive the right Region,
  and do not share the source Region size. CPU half-splits use the existing
  split-check path.

## Observability

`split_success` and `split_failed` account for load-split attempts that reach
PD split-ID allocation or admin-request handling. Earlier rejections, including
PD-task scheduler failures, are not counted. CPU half-splits use split-check
outcome metrics.

## Change Management And Impact

Changes to candidate routing, peer identity, Region validation, or callbacks
should be reviewed across the PD worker, peer FSM, and router. Preserve the
separate raftstore-v2 behavior when changing shared candidate types.

## Reading Map

1. `components/raftstore/src/store/worker/pd.rs`
2. `components/raftstore/src/store/msg.rs`
3. `components/raftstore/src/router.rs`
4. `components/raftstore/src/store/fsm/peer.rs`
5. `components/raftstore/src/store/util.rs`

The inline `test_auto_split*` tests cover message construction, source
semantics, and Region validation.

## Glossary

- Peer FSM: the local state machine for a Region replica.
- PD: the service that supplies Region metadata and split IDs.

---
title: PD Scheduler Configuration
summary: Learn how to configure PD Scheduler.
category: operations
---

# PD Scheduler Configuration

PD Scheduler is responsible for scheduling the storage and computing resources. PD has many kinds of schedulers to meet the requirements in different scenarios. PD Scheduler is one of the most important component in PD.

The basic workflow of PD Scheduler is as follows. First, the scheduler is triggered according to `minAdjacentSchedulerInterval` defined in `ScheduleController`. Then it tries to select the source store and the target store, create the corresponding operators and send a message to TiKV to do some operations.

## Usage description

This section describes the usage of PD Scheduler parameters.

### `max-merge-region-keys && max-merge-region-size`

If the Region size is smaller than `max-merge-region-size` and the number of keys in the Region is smaller than `max-merge-region-keys` at the same time, the Region will try to merge with adjacent Regions. The default value of both the two parameters is 0. Currently, `merge` is not enabled by default.

### `split-merge-interval`

`split-merge-interval` is the minimum interval time to allow merging after split. The default value is "1h".

### `max-snapshot-count`

If the snapshot count of one store is larger than the value of `max-snapshot-count`, it will never be used as a source or target store. The default value is 3.

### `max-pending-peer-count`

If the pending peer count of one store is larger than the value of `max-pending-peer-count`, it will never be used as a source or target store. The default value is 16.

### `max-store-down-time`

`max-store-down-time` is the maximum duration after which a store is considered to be down if it has not reported heartbeats. The default value is “30m”.

### `leader-schedule-limit`

`leader-schedule-limit` is the maximum number of coexistent leaders that are under scheduling. The default value is 4.

### `region-schedule-limit`

`region-schedule-limit` is the maximum number of coexistent Regions that are under scheduling. The default value is 4.

### `replica-schedule-limit`

`replica-schedule-limit` is the maximum number of coexistent replicas that are under scheduling. The default value is 8.

### `merge-schedule-limit`

`merge-schedule-limit` is the maximum number of coexistent merges that are under scheduling. The default value is 8.

### `tolerant-size-ratio`

`tolerant-size-ratio` is the ratio of buffer size for the balance scheduler. The default value is 5.0.

### `low-space-ratio`

`low-space-ratio` is the lowest usage ratio of a storage which can be regarded as low space. When a storage is in low space, the score turns to be high and varies inversely with the available size.

### `high-space-ratio`

`high-space-ratio` is the highest usage ratio of storage which can be regarded as high space. High space means there is a lot of available space of the storage, and the score varies directly with the used size.

### `disable-raft-learner`

`disable-raft-learner` is the option to disable `AddNode` and use `AddLearnerNode` instead.

### `disable-remove-down-replica`

`disable-remove-down-replica` is the option to prevent replica checker from removing replicas whose status are down.

### `disable-replace-offline-replica`

`disable-replace-offline-replica` is the option to prevent the replica checker from replacing offline replicas.

### `disable-make-up-replica`

`disable-make-up-replica` is the option to prevent the replica checker from making up replicas when the count of replicas is less than expected.

### `disable-remove-extra-replica`

`disable-remove-extra-replica` is the option to prevent the replica checker from removing extra replicas.

### `disable-location-replacement`

`disable-location-replacement` is the option to prevent the replica checker from moving the replica to a better location.

## Customization

The default schedulers include `balance-leader`, `balance-region` and `hot-region`. In addition, you can also customize the schedulers. For each scheduler, the configuration has three variables: `type`, `args` and `disable`.

Here is an example to enable the `evict-leader` scheduler in the `config.toml` file:

```
[[schedule.schedulers]]
type = "evict-leader"
args = ["1"]
disable = false
```
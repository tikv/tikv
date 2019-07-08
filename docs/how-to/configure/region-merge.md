---
title: Region Merge
summary: Learn how to configure Region Merge in TiKV.
category: how-to
---

# Region Merge

TiKV replicates a segment of data in Regions via the Raft state machine. As data writes increase, a Region Split happens when the size of the region or the number of keys has reached a threshold. Conversely, if the size of the Region or the amount of keys shrinks because of data deletion, we can use Region Merge to merge adjacent regions that are smaller. This relieves some stress on Raftstore.


## Merge process

Region Merge is initiated by the Placement Driver (PD). The steps are:

1. PD polls the sizes and number of keys of all regions on the TiKV node.

2. When the region size is less than `max-merge-region-size` or the number of keys the region includes is less than `max-merge-region-keys`, PD performs Region Merge on the smaller of the two adjacent Regions.

> **Note:**
>
> - All replicas of the merged Regions must belong to the same set of TiKVs.
> - Newly split Regions won't be merged within the period of time specified by `split-merge-interval`.
> - Region Merge won't happen within the period of time specified by `split-merge-interval` after PD starts or restarts.
>- Region Merge won't happen for two Regions that belong to different tables if `namespace-classifier = table` (default).

## Configure Region Merge

Region Merge is enabled by default. You can use `pd-ctl` or the PD configuration file to configure Region Merge.

To enable Region Merge, set the following parameters to a non-zero value:

- `max-merge-region-size`
- `max-merge-region-keys`
- `merge-schedule-limit`

You can use `split-merge-interval` to control the interval between the `split` and `merge` operations.

For detailed descriptions on the above parameters, refer to [PD Control](../../reference/tools/pd-control.md).

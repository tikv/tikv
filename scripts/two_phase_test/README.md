# Two-Phase Scheduling Test

Tests RFC #123 fair scheduling based on historical RU consumption.

## Prerequisites

- tiup playground running with custom tikv-server:
  ```
  tiup playground nightly \
    --kv 1 --kv.config /tmp/kv.toml \
    --kv.binpath /path/to/tikv_lat/target/debug/tikv-server \
    --db 1 --pd 1 --tiflash 0
  ```
- `/tmp/kv.toml` must have:
  ```toml
  [readpool.unified]
  min-thread-count = 1
  max-thread-count = 1

  [resource-control]
  enable-dynamic-reservation = true
  ru-historical-window = "10m"
  ```

## Running the test

**Terminal 1** – steady workload (run for 12+ minutes to build history):
```
bash steady.sh
```

**Terminal 2** – monitor latency:
```
bash measure.sh
```

**Terminal 3** – after 12 minutes, fire the spike:
```
bash spike.sh
```

## Expected result

With two-phase scheduling enabled:
- `steady` group stays in **phase 0** (within historical reservation) → low latency ~baseline
- `spike` group jumps to **phase 1** (exceeds reservation) → queued behind steady → higher latency

The spike's avg_ms should be noticeably higher than steady's avg_ms during the burst.

## Setup (first time or after playground restart)

```
bash setup.sh
```

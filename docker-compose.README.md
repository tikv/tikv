# TiKV Cluster with Docker Compose

This docker-compose setup provides a complete TiKV cluster with 3 PD (Placement Driver) nodes and 3 TiKV nodes, all running on a single machine for development and testing.

## Prerequisites

- Docker Engine 20.10 or later
- Docker Compose v2 (or docker-compose 1.29+)
- **8GB+ of available RAM** (required for building TiKV from source)
  - If building fails, see [troubleshooting](#build-fails-with-cannot-allocate-memory-error) for alternatives
- Sufficient disk space for data volumes (at least 10GB recommended)

> **Note:** On macOS, Docker Desktop uses `docker compose` (with a space) instead of `docker-compose` (with a hyphen). All commands in this README use `docker-compose` for compatibility, but macOS users should replace it with `docker compose` in all commands.

## Quick Start

> **macOS Note:** If you're on macOS, replace `docker-compose` with `docker compose` (space instead of hyphen) in all commands below.

1. **Start the cluster** (uses pre-built TiKV and PD nightly images from Docker Hub):
   ```bash
   docker-compose up -d
   # On macOS, use: docker compose up -d
   ```

2. **Check cluster status**:
   ```bash
   docker-compose ps
   ```

3. **View logs**:

   There are two ways to view logs:

   **Option A: Using docker-compose logs (stdout/stderr)**
   ```bash
   # All services
   docker-compose logs -f
   # On macOS, use: docker compose logs -f
   
   # Specific service
   docker-compose logs -f tikv1
   docker-compose logs -f pd1
   ```

   **Option B: Access log files directly from containers**
   ```bash
   # View PD logs
   docker exec tikv-pd1 cat /logs/pd.log
   docker exec tikv-pd2 cat /logs/pd.log
   docker exec tikv-pd3 cat /logs/pd.log
   
   # View TiKV logs
   docker exec tikv-tikv1 cat /logs/tikv.log
   docker exec tikv-tikv2 cat /logs/tikv.log
   docker exec tikv-tikv3 cat /logs/tikv.log
   
   # Follow logs in real-time (tail -f)
   docker exec tikv-pd1 tail -f /logs/pd.log
   docker exec tikv-tikv1 tail -f /logs/tikv.log
   
   # View last N lines
   docker exec tikv-pd1 tail -n 100 /logs/pd.log
   docker exec tikv-tikv1 tail -n 100 /logs/tikv.log
   ```

   **Option C: Access logs via volume inspection**
   ```bash
   # Find log volume location
   docker volume inspect tikv_pd1-logs
   docker volume inspect tikv_tikv1-logs
   
   # On Linux, you can access the volume directly:
   # sudo ls -la /var/lib/docker/volumes/tikv_pd1-logs/_data
   ```

4. **Stop the cluster**:
   ```bash
   docker-compose down
   ```

5. **Stop and remove all data** (clean slate):
   ```bash
   docker-compose down -v
   ```

## Service Ports

### PD (Placement Driver) Services
- **PD1**: Client API: `23791`, Peer: `23801`
- **PD2**: Client API: `23792`, Peer: `23802`
- **PD3**: Client API: `23793`, Peer: `23803`

### TiKV Services
- **TiKV1**: Server: `20161`, Status: `20181`
- **TiKV2**: Server: `20162`, Status: `20182`
- **TiKV3**: Server: `20163`, Status: `20183`

## Accessing the Cluster

### PD Dashboard
Access the PD dashboard via any PD client URL:
- http://localhost:23791/dashboard
- http://localhost:23792/dashboard
- http://localhost:23793/dashboard

### TiKV Status
Check TiKV status endpoints:
- http://localhost:20181/status
- http://localhost:20182/status
- http://localhost:20183/status

### Using TiKV Client

The PD endpoints for client connections are:
- `127.0.0.1:23791`
- `127.0.0.1:23792`
- `127.0.0.1:23793`

Example with Python client:
```python
from tikv_client import RawClient

# Connect to any PD endpoint
client = RawClient.connect(["127.0.0.1:23791"])

# Test operations
client.put(b'foo', b'bar')
print(client.get(b'foo'))  # b'bar'
```

## Log Management

Logs are stored in separate volumes from data, located at `/logs` inside each container:

- **PD logs**: `/logs/pd.log` in each PD container
- **TiKV logs**: `/logs/tikv.log` in each TiKV container

### Viewing Log Files

**View entire log file:**
```bash
docker exec tikv-pd1 cat /logs/pd.log
docker exec tikv-tikv1 cat /logs/tikv.log
```

**Follow logs in real-time (like `tail -f`):**
```bash
docker exec tikv-pd1 tail -f /logs/pd.log
docker exec tikv-tikv1 tail -f /logs/tikv.log
```

**View last N lines:**
```bash
docker exec tikv-pd1 tail -n 100 /logs/pd.log
docker exec tikv-tikv1 tail -n 100 /logs/tikv.log
```

**Search logs for specific content:**
```bash
docker exec tikv-pd1 grep "error" /logs/pd.log
docker exec tikv-tikv1 grep "ERROR" /logs/tikv.log
```

**View logs from all nodes:**
```bash
# All PD logs
for i in 1 2 3; do echo "=== PD$i ==="; docker exec tikv-pd$i tail -n 20 /logs/pd.log; done

# All TiKV logs
for i in 1 2 3; do echo "=== TiKV$i ==="; docker exec tikv-tikv$i tail -n 20 /logs/tikv.log; done
```

### Log Volume Locations

Logs are stored in Docker volumes. To find their locations:

```bash
# List all log volumes
docker volume ls | grep logs

# Inspect a specific log volume
docker volume inspect tikv_pd1-logs
docker volume inspect tikv_tikv1-logs
```

On Linux, you can access volumes directly at `/var/lib/docker/volumes/`. On macOS/Windows, volumes are managed by Docker Desktop.

### Log Rotation and Cleanup

Log files will grow over time. To manage them:

**Clear logs (restart container to recreate log file):**
```bash
docker exec tikv-pd1 truncate -s 0 /logs/pd.log
docker exec tikv-tikv1 truncate -s 0 /logs/tikv.log
```

**Remove log volumes (removes all logs):**
```bash
docker-compose down -v  # Removes all volumes including logs
# Or remove specific log volumes:
docker volume rm tikv_pd1-logs tikv_pd2-logs tikv_pd3-logs
docker volume rm tikv_tikv1-logs tikv_tikv2-logs tikv_tikv3-logs
```

## Data Persistence

All data and logs are stored in Docker volumes:

**Data volumes:**
- `pd1-data`, `pd2-data`, `pd3-data`: PD cluster data
- `tikv1-data`, `tikv2-data`, `tikv3-data`: TiKV store data

**Log volumes:**
- `pd1-logs`, `pd2-logs`, `pd3-logs`: PD log files
- `tikv1-logs`, `tikv2-logs`, `tikv3-logs`: TiKV log files

To view volume locations:
```bash
docker volume ls | grep tikv
docker volume inspect tikv_pd1-data
docker volume inspect tikv_pd1-logs
```

## Troubleshooting

### Build fails with "cannot allocate memory" error

Building TiKV from source requires significant memory (8GB+ RAM recommended). If you encounter memory errors:

> **Why so much memory?** See [BUILD_MEMORY_EXPLANATION.md](../BUILD_MEMORY_EXPLANATION.md) for a detailed explanation of why TiKV builds are memory-intensive (Rust compilation, LTO, RocksDB, etc.).

**Solution 1: Increase Docker memory limit**
- **Docker Desktop (Mac/Windows)**: Go to Settings → Resources → Advanced, increase Memory to at least 8GB
- **Linux**: Ensure Docker has access to sufficient system memory

**Solution 2: Build the image separately with more resources**
```bash
# Build with increased memory limit (if using Docker buildx)
docker buildx build --memory=8g -t tikv:latest -f Dockerfile .

# Or build normally if your system has enough RAM
docker build -t tikv:latest -f Dockerfile .
```

**Solution 3: Use a pre-built TiKV image**
The docker-compose.yml already uses pre-built nightly images (`pingcap/tikv:nightly` and `pingcap/pd:nightly`) by default. If you want to use a different version, edit `docker-compose.yml` and change the image tags.

To use the latest stable version instead of nightly:
- Change `pingcap/tikv:nightly` to `pingcap/tikv:latest`
- Change `pingcap/pd:nightly` to `pingcap/pd:latest`

Then start the cluster:
```bash
docker-compose up -d
```

**Solution 4: Build on a machine with more resources**
Build the image on a machine with more RAM, then push to a registry or export/import:
```bash
# On build machine
docker build -t tikv:latest -f Dockerfile .
docker save tikv:latest | gzip > tikv-image.tar.gz

# On target machine
docker load < tikv-image.tar.gz
```

### PD startup warnings about DNS and "server not started"

You may see warnings in PD logs during startup:

**DNS resolution warnings:**
```
[WARN] [probing_status.go:68] ["prober detected unhealthy status"]
[error="dial tcp: lookup pd2 on 127.0.0.11:53: no such host"]
```

**Leader sync warnings (most common):**
```
[WARN] [client.go:172] ["region sync with leader meet error"]
[error="[PD:grpc:ErrGRPCRecv]receive response error: rpc error: code = Unavailable desc = server not started"]
```

**Cluster not bootstrapped warning:**
```
[WARN] [cluster.go:370] ["cluster is not bootstrapped"]
```

**These are normal and expected!** Here's why:

1. **DNS warnings**: All PD nodes start simultaneously to allow proper Raft leader election. Initially, containers may try to resolve each other's hostnames before all containers are fully connected to the network. These resolve once all containers are running.

2. **"Server not started" / "Cluster not bootstrapped" warnings**: 
   - PD cluster needs to be **bootstrapped** by the first TiKV node that connects
   - Until TiKV nodes start and bootstrap the cluster, PD has no regions to manage
   - pd2 and pd3 are trying to sync regions with the leader, but there are no regions yet
   - **These warnings will stop automatically once TiKV nodes start and bootstrap the cluster**

**What to check:**
1. Verify PD leader is ready: Check pd1 logs for `"PD leader is ready to serve"`
2. Verify all PD nodes are healthy: `docker-compose ps`
3. Check PD members: `curl http://localhost:23791/pd/api/v1/members` (should show all 3 PDs)
4. **Start TiKV nodes**: The warnings will stop once TiKV nodes connect and bootstrap the cluster
5. After TiKV starts, wait 30-60 seconds and check logs again - warnings should disappear
6. If warnings persist after TiKV is running for 2+ minutes, check for actual errors

**Expected behavior:**
- PD nodes start and elect a leader ✅
- Warnings appear about "server not started" and "cluster not bootstrapped" ⚠️ (expected)
- TiKV nodes start and connect to PD ✅
- First TiKV node bootstraps the cluster ✅
- Warnings stop appearing ✅

The docker-compose.yml is configured to start all PD nodes simultaneously, which is correct for Raft cluster formation. The warnings are temporary and will resolve once TiKV nodes bootstrap the cluster.

### Check if services are healthy
```bash
docker-compose ps
```

### View detailed logs
```bash
docker-compose logs tikv1
docker-compose logs pd1
```

### Restart a specific service
```bash
docker-compose restart tikv1
```

### Build TiKV from source (if you need custom builds)

The docker-compose.yml uses the pre-built `pingcap/tikv:nightly` image by default. To build from source:

1. Edit `docker-compose.yml` and for each TiKV service:
   - Remove the `image: pingcap/tikv:latest` line
   - Add a `build:` section:
     ```yaml
     build:
       context: .
       dockerfile: Dockerfile
     image: tikv:latest
     ```

2. Build and start:
   ```bash
   docker-compose build tikv1 tikv2 tikv3
   docker-compose up -d
   ```

**Note:** Building from source requires 8GB+ RAM. See [troubleshooting](#build-fails-with-cannot-allocate-memory-error) for details.

### Check cluster health via PD API
```bash
curl http://localhost:23791/pd/api/v1/health
```

## Development Tips

1. **Using pre-built images**: The default setup uses `pingcap/tikv:nightly` and `pingcap/pd:nightly` from Docker Hub, so no building is required. Just start the cluster with `docker-compose up -d`.

2. **Custom builds**: If you need to build from source (e.g., for development), modify `docker-compose.yml` to add build sections, then:
   ```bash
   docker-compose build tikv1 tikv2 tikv3
   docker-compose restart tikv1 tikv2 tikv3
   ```

3. **Debugging**: To debug a specific TiKV node, you can exec into the container:
   ```bash
   docker exec -it tikv-tikv1 /bin/bash
   ```

4. **Configuration**: To modify TiKV configuration, you can:
   - Create custom config files and mount them as volumes
   - Use command-line arguments in the docker-compose.yml
   - Create a config file and pass it with `--config` flag

5. **Performance tuning**: For development, you may want to reduce resource usage by:
   - Reducing memory limits in docker-compose.yml
   - Adjusting TiKV configuration for smaller datasets
   - Using fewer replicas (modify the compose file)

## Network

All services communicate via the `tikv-network` bridge network. Services can reach each other using their service names (e.g., `pd1`, `tikv1`).

## Cleanup

To completely remove everything including volumes:
```bash
docker-compose down -v
docker system prune -f
```


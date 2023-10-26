# HTTP API

In the context of the following line: `TIKV_ADDRESS=$TIKV_IP:$TIKV_STATUS_PORT`

By default:

- `TIKV_IP` should be set to `127.0.0.1`
- `TIKV_STATUS_PORT` should be set to `20180`

## CPU Profiling

Collect and export CPU profiling data within a specified time range.

```bash
curl -H 'Content-Type:<type>' -X GET 'http://$TIKV_ADDRESS/debug/pprof/profile?seconds=<seconds>&frequency=<frequency>'
```

#### Parameters

- **seconds** (optional): Specifies the number of seconds to collect CPU profiling data.
  - Default: 10
  - Example: `?seconds=20`

- **frequency** (optional): Specifies the sampling frequency for CPU profiling data.
  - Default: 99
  - Example: `?frequency=100`

- **type** (optional): Specifies the Content-Type of the response.
  - Options: `application/protobuf` for raw profile data, any other types for flame graph.
  - Default: `N/A`
  - Example: `-H "Content-Type:application/protobuf"`

#### Response

The server will return CPU profiling data. The response format is determined by the Content-Type in the request header and can be either raw profile data in protobuf format or flame graph in SVG format.

The raw profile data can be handled by `pprof` tool. For example, use `go tool pprof --http=0.0.0.0:1234 xxx.proto` to open a interactive web browser.

## Heap Profiling

### Activate Heap Profiling

Activate heap profiling of jemalloc. When activated, jemalloc would collect memory usage at malloc, demalloc, etc., walking the call stack to capture a backtrace. So it would affect performance in some extent.

```bash
curl -X GET 'http://$TIKV_ADDRESS/debug/pprof/heap_activate?interval=<interval>'
```

#### Parameters

- **interval** (optional): Specifies the interval (in seconds) for dumping heap profiles in a temporary directory under TiKV data directory. If set to 0, period dumping is disable. You can dump heap profiles manually by the other API.
  - Default: 0
  - Example: `?interval=60`

#### Response

A confirmation message indicating whether heap profiling activation was successful. If it has been already activated, it would return a error message without any side effect.

### Deactivate Heap Profiling

Deactivate the currently running heap profiling.

```bash
curl -X GET 'http://$TIKV_ADDRESS/debug/pprof/heap_deactivate'
```

#### Response

If heap profiling is active, it will be stopped. The server will return a message indicating whether the deactivation was successful.
If heap profiling is not currently active, the server will return a message indicating that no heap profiling is running.

### List Heap Profiles

List available heap profiling profiles which are periodically dumped when activated by `heap_activate` API with `interval` specified.

Note that, once deactivation is performed, all existing profiles will be deleted.

```bash
curl -X GET 'http://$TIKV_ADDRESS/debug/pprof/heap_list'
```

#### Response

It will return a list of profiles, each represented as a file name and last modification timestamp, in plain text format. The profiles are sorted in reverse order based on their modification timestamps.

If there are no available heap profiles or heap profiling is inactive, the server will return an empty list.

### Retrieve Heap Profile

Collect and export heap profiling data.

Note that, heap profile is not like CPU profile which is collected within the specified time range right after the request. Instead, heap profile is just a snapshot of the accumulated memory usage at the time of request, as the memory usage is always being collected once activated.

```bash
curl -X GET 'http://$TIKV_ADDRESS/debug/pprof/heap?name=<name>&jeprof=<true>'
```

#### Parameters

- **name** (optional): Specifies the name of the heap profile to retrieve. If not specified, a heap profile will be retrieved.
  - Default: ``
  - Example: `?name=000001.heap`

- **jeprof** (optional): Indicates whether to use Jeprof to process the heap profile to generate call graph. It needs `perl` being installed.
  - Default: false
  - Example: `?jeprof=true`

#### Response

The server will return heap profiling data. The response format is determined by the `jeprof` parameter. If true, the response will be a call graph in SVG format. Otherwise, the response will be raw profile data in jemalloc dedicated format.

### Heap Profile Symbolization

The heap profile retrieved by `heap` API by default is a raw profile data in jemalloc dedicated format, which should be handled by `jeporf` to visualize.

There are two ways to generate a call graph in SVG format from the raw profile data:

- local: by provided profile and use TiKV binary to resolve symbols

```bash
jeprof --svg <binary> <profile>
```

- remote: by latest heap profile retrieved by HTTP and use symbolization service provided by TiKV to resolve symbols

```bash
jeprof --svg http://$TIKV_ADDRESS/debug/pprof/heap 
```

To support the remote way, TiKV provides a symbolization service to resolve symbols from memory addresses. Jeprof would implicitly call the `.../debug/pprof/symbol` to map call stack's addresses to corresponding function names. For most of the cases, you don't need to
it explicitly. But if you want to use it for other purposes, you can refer as follows.

```bash
curl -X POST -d '<address_list>' 'http://$TIKV_ADDRESS/debug/pprof/symbol'
```

#### Parameters

- **address_list** (required): A list of memory addresses to be resolved. The addresses should be provided in hexadecimal format(whether or not start with '0x' is okay), separated by a '+' character.

#### Response

A list of resolved symbols in plain text. Each line represented as a hexadecimal address followed by the corresponding function name. If a memory address cannot be resolved, it will be marked with "??".


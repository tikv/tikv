# Performance Critical Path of read or write requests
There're some functions that are in the critical path of read or write requests. They're so important to the overall performance that any regression will directly impact user experience. We added a comment [PerformanceCriticalPath] to highlight these critical functions. Please note that not all functions in critical path are marked, for example many short but very common functions are not marked. So please pay special attention when you change code that is itself marked as [PerformanceCriticalPath] or called by functions marked as [PerformanceCriticalPath]. 
Here're some typical mistakes that should be avoided in these functions.
* Unnecessary synchronous I/O.
* Verbose logging with infor or above log level.
* Global lock.
* Long tasks that do not have to be synchronous.(Could be done in background thread instead.)

Here's the [performance map](https://github.com/pingcap/tidb-map/blob/master/maps/performance-map.png) that would help you to understand how read/write requests are executed inside Tikv. 
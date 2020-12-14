// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#include <uapi/linux/ptrace.h>
#include <linux/blkdev.h>

struct stats_t {
    u64 read;
    u64 write;
};

// Should be consistent with IOType in `file_system/lib.rs`
typedef enum {
    Other,
    Read,
    Write,
    Coprocessor,
    Flush,
    Compaction,
    Replication,
    LoadBalance,
    Import,
    Export,
} io_type;

struct info_t {
    u64 start_ts;
    io_type type;
};

BPF_HASH(info_by_req, struct request *, struct info_t);
BPF_HASH(type_by_pid, u32, io_type*);
BPF_HASH(stats_by_type, io_type, struct stats_t);

// the latency includes OS queued time
// When using BPF_ARRAY_OF_MAPS, it can't call something like hist_arrays[idx].increment() due .
BPF_HISTOGRAM(other_latency);
BPF_HISTOGRAM(read_latency);
BPF_HISTOGRAM(write_latency);
BPF_HISTOGRAM(coprocessor_latency);
BPF_HISTOGRAM(flush_latency);
BPF_HISTOGRAM(compaction_latency);
BPF_HISTOGRAM(replication_latency);
BPF_HISTOGRAM(loadbalance_latency);
BPF_HISTOGRAM(import_latency);
BPF_HISTOGRAM(export_latency);

// cache PID by req
int trace_pid_start(struct pt_regs *ctx, struct request *req)
{
    u64 id = bpf_get_current_pid_tgid();
    u32 pid = id & (((u64)1 << 32) - 1);
    u32 tgid = id >> 32;
    if (tgid != ##TGID##) {
        return 0;
    }

    io_type** type_ptr = type_by_pid.lookup(&pid);
    if (type_ptr == 0) return 0;
    struct info_t info;
    int err = bpf_probe_read(&info.type, sizeof(io_type), (void*)*type_ptr);
    if (err != 0) {
        info.type = Other;
        bpf_trace_printk("pid %d error %d here\n", pid, err);
    }
    info.ts = bpf_ktime_get_ns();

    info_by_req.update(&req, &info);
    return 0;
}

// trace_req_completion may be called in interrput context. In that case, 
// `bpf_get_current_pid_tgid` and `bpf_probe_read` can not work as expected.
// So caching type in `trace_pid_start` which wouldn't be called in interrupt
// context and query the type by req in `info_by_req`.
int trace_req_completion(struct pt_regs *ctx, struct request *req)
{
    struct info_t* info = info_by_req.lookup(&req);
    if (info == 0) return 0;
/*
 * The following deals with a kernel version change (in mainline 4.7, although
 * it may be backported to earlier kernels) with how block request write flags
 * are tested. We handle both pre- and post-change versions here. Please avoid
 * kernel version tests like this as much as possible: they inflate the code,
 * test, and maintenance burden.
 */
    u64 rwflag;
#ifdef REQ_WRITE
    rwflag = !!(req->cmd_flags & REQ_WRITE);
#elif defined(REQ_OP_SHIFT)
    rwflag = !!((req->cmd_flags >> REQ_OP_SHIFT) == REQ_OP_WRITE);
#else
    rwflag = !!((req->cmd_flags & REQ_OP_MASK) == REQ_OP_WRITE);
#endif
    io_type type = info->type;
    struct stats_t zero = {}, *val;
    val = stats_by_type.lookup_or_init(&type, &zero);
    if (rwflag == 1) {
        (*val).write += req->__data_len;
    } else {
        (*val).read += req->__data_len;
    }

    u64 delta = (bpf_ktime_get_ns() - info.start_ts) / 1000; // microseconds
    switch (type) {
        case Other:
            other_latency.increment(bpf_log2l(delta));
            break;
        case Read:
            read_latency.increment(bpf_log2l(delta));
            break;
        case Write:
            write_latency.increment(bpf_log2l(delta));
            break;
        case Coprocessor:
            coprocessor_latency.increment(bpf_log2l(delta));
            break;
        case Flush:
            flush_latency.increment(bpf_log2l(delta));
            break;
        case Compaction:
            compaction_latency.increment(bpf_log2l(delta));
            break;
        case Replication:
            replication_latency.increment(bpf_log2l(delta));
            break;
        case LoadBalance:
            loadbalance_latency.increment(bpf_log2l(delta));
            break;
        case Import:
            import_latency.increment(bpf_log2l(delta));
            break;
        case Export:
            export_latency.increment(bpf_log2l(delta));
            break;
    }

    info_by_req.delete(&req);
    return 0;
}
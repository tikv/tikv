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
    io_type type;
    u64 start_ts;
};

BPF_HASH(info_by_req, struct request *, struct info_t);
BPF_HASH(type_by_pid, u32, io_type*);
BPF_HASH(stats_by_type, io_type, struct stats_t);

// the latency includes OS queued time
// When using BPF_ARRAY_OF_MAPS, hist_arrays[idx].increment() is not availble
// due to bpf API limition. So define separate hists for every io type.
BPF_HISTOGRAM(other_read_latency, int, 25);
BPF_HISTOGRAM(read_read_latency, int, 25);
BPF_HISTOGRAM(write_read_latency, int, 25);
BPF_HISTOGRAM(coprocessor_read_latency, int, 25);
BPF_HISTOGRAM(flush_read_latency, int, 25);
BPF_HISTOGRAM(compaction_read_latency, int, 25);
BPF_HISTOGRAM(replication_read_latency, int, 25);
BPF_HISTOGRAM(loadbalance_read_latency, int, 25);
BPF_HISTOGRAM(import_read_latency, int, 25);
BPF_HISTOGRAM(export_read_latency, int, 25);

BPF_HISTOGRAM(other_write_latency, int, 25);
BPF_HISTOGRAM(read_write_latency, int, 25);
BPF_HISTOGRAM(write_write_latency, int, 25);
BPF_HISTOGRAM(coprocessor_write_latency, int, 25);
BPF_HISTOGRAM(flush_write_latency, int, 25);
BPF_HISTOGRAM(compaction_write_latency, int, 25);
BPF_HISTOGRAM(replication_write_latency, int, 25);
BPF_HISTOGRAM(loadbalance_write_latency, int, 25);
BPF_HISTOGRAM(import_write_latency, int, 25);
BPF_HISTOGRAM(export_write_latency, int, 25);

// cache PID by req
int trace_pid_start(struct pt_regs *ctx, struct request *req)
{
    // TODO: Using bpf_get_ns_current_pid_tgid of newer kernel to get pid under namespace
    // 
    // struct bpf_pidns_info ns = {};
    // if (!bpf_get_ns_current_pid_tgid(##DEV##, ##INO##, &ns, sizeof(struct bpf_pidns_info))) {
    //     return 0;
    // }
    // u32 pid = ns.pid;
    // u32 tgid = ns.tgid;

    u64 id = bpf_get_current_pid_tgid();
    u32 pid = id & (((u64)1 << 32) - 1);
    u32 tgid = id >> 32;
    if (tgid != ##TGID##) {
        return 0;
    }

    io_type** type_ptr = type_by_pid.lookup(&pid);
    if (type_ptr == 0) return 0;
    struct info_t info;
    __builtin_memset(&info, 0, sizeof(info)); // ensure padding is always zeroed, otherwise verifier will complain.
    int err = bpf_probe_read(&info.type, sizeof(io_type), (void*)*type_ptr);
    if (err != 0) {
        info.type = Other;
        bpf_trace_printk("pid %d error %d here\n", pid, err);
    }
    info.start_ts = bpf_ktime_get_ns();

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

    u64 delta = (bpf_ktime_get_ns() - info->start_ts) / 1000; // microseconds
    switch (type) {
        case Other:
            if (rwflag == 1) {
                other_write_latency.increment(bpf_log2l(delta));
            } else {
                other_read_latency.increment(bpf_log2l(delta));
            }
            break;
        case Read:
            if (rwflag == 1) {
                read_write_latency.increment(bpf_log2l(delta));
            } else {
                read_read_latency.increment(bpf_log2l(delta));
            }
            break;
        case Write:
            if (rwflag == 1) {
                write_write_latency.increment(bpf_log2l(delta));
            } else {
                write_read_latency.increment(bpf_log2l(delta));
            }
            break;
        case Coprocessor:
            if (rwflag == 1) {
                coprocessor_write_latency.increment(bpf_log2l(delta));
            } else {
                coprocessor_read_latency.increment(bpf_log2l(delta));
            }
            break;
        case Flush:
            if (rwflag == 1) {
                flush_write_latency.increment(bpf_log2l(delta));
            } else {
                flush_read_latency.increment(bpf_log2l(delta));
            }
            break;
        case Compaction:
            if (rwflag == 1) {
                compaction_write_latency.increment(bpf_log2l(delta));
            } else {
                compaction_read_latency.increment(bpf_log2l(delta));
            }
            break;
        case Replication:
            if (rwflag == 1) {
                replication_write_latency.increment(bpf_log2l(delta));
            } else {
                replication_read_latency.increment(bpf_log2l(delta));
            }
            break;
        case LoadBalance:
            if (rwflag == 1) {
                loadbalance_write_latency.increment(bpf_log2l(delta));
            } else {
                loadbalance_read_latency.increment(bpf_log2l(delta));
            }
            break;
        case Import:
            if (rwflag == 1) {
                import_write_latency.increment(bpf_log2l(delta));
            } else {
                import_read_latency.increment(bpf_log2l(delta));
            }
            break;
        case Export:
            if (rwflag == 1) {
                export_write_latency.increment(bpf_log2l(delta));
            } else {
                export_read_latency.increment(bpf_log2l(delta));
            }
            break;
    }

    info_by_req.delete(&req);
    return 0;
}
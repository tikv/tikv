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
};

BPF_HASH(infobyreq, struct request *, struct info_t);
BPF_HASH(typebypid, u32, io_type*);
BPF_HASH(statsbytype, io_type, struct val_t);
BPF_HISTOGRAM()

int trace_pid_start(struct pt_regs *ctx, struct request *req)
{
    u64 id = bpf_get_current_pid_tgid();
    u32 pid = id & (((u64)1 << 32) - 1);
    u32 tgid = id >> 32;
    if (tgid != ##TGID##) {
        return 0;
    }

    io_type** type_ptr = typebypid.lookup(&pid);
    if (type_ptr == 0) return 0;
    struct info_t info;
    int err = bpf_probe_read(&info.type, sizeof(io_type), (void*)*type_ptr);
    if (err != 0) {
        info.type = io_type::Other;
        bpf_trace_printk("pid %d error %d here\n", pid, err);
    }

    infobyreq.update(&req, &info);
    return 0;
}

// trace_req_completion may be called in interrput context. In that case, 
// `bpf_get_current_pid_tgid` and `bpf_probe_read` can not work as expected.
// So caching type in `trace_pid_start` which wouldn't be called in interrupt
// context and query the type by req in `infobyreq`.
int trace_req_completion(struct pt_regs *ctx, struct request *req)
{
    struct info_t* info = infobyreq.lookup(&req);
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
    struct val_t zero = {}, *val;
    val = statsbytype.lookup_or_init(&type, &zero);
    if (rwflag == 1) {
        (*val).write += req->__data_len;
    } else {
        (*val).read += req->__data_len;
    }
    infobyreq.delete(&req);
    return 0;
}
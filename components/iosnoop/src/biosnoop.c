// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#include <uapi/linux/ptrace.h>
#include <linux/blkdev.h>

struct val_t {
    u64 read;
    u64 write;
};

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

BPF_HASH(pidbyreq, struct request *, u32);
BPF_HASH(typebypid, u32, io_type*);
BPF_HASH(statsbytype, io_type, struct val_t);

int trace_pid_start(struct pt_regs *ctx, struct request *req)
{
    u64 id = bpf_get_current_pid_tgid();
    u32 pid = id & (((u64)1 << 32) - 1);
    u32 tgid = id >> 32;
    if (tgid != ##TGID##) {
        return 0;
    }
    pidbyreq.update(&req, &pid);
    return 0;
}

// output
int trace_req_completion(struct pt_regs *ctx, struct request *req)
{
    u32* pid = pidbyreq.lookup(&req);
    if (pid == 0) return 0;
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
    u32 p = *pid; // if not copy, bpf verifier will not pass
    bpf_trace_printk("complet pid %d\n", p);
    io_type** type_ptr = typebypid.lookup(&p);
    if (type_ptr == 0) return 0;
    io_type type;
    bpf_trace_printk("complet pid %d type %d, type %p\n", p, type, *type_ptr);
    int err= bpf_probe_read(&type, sizeof(type), (void*)*type_ptr);
    if (err  != 0) {
        bpf_trace_printk("error %d here\n", err);
        return 0;
    }

    struct val_t zero = {}, *val;
    val = statsbytype.lookup_or_init(&type, &zero);
    if (rwflag == 1) {
        (*val).write += req->__data_len;
    } else {
        (*val).read += req->__data_len;
    }
    pidbyreq.delete(&req);
    return 0;
}
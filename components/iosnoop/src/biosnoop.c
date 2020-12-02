// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#include <uapi/linux/ptrace.h>
#include <linux/blkdev.h>

struct val_t {
    u64 read;
    u64 write;
};

BPF_HASH(pidbyreq, struct request *, u32);
BPF_HASH(statsbypid, u32, struct val_t);

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
    struct val_t zero =  {}, *val;
    val = statsbypid.lookup_or_init(&p, &zero);
    if (rwflag == 1) {
        (*val).write += req->__data_len;
    } else {
        (*val).read += req->__data_len;
    }
    pidbyreq.delete(&req);
    return 0;
}
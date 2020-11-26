// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#include <uapi/linux/ptrace.h>
#include <linux/blkdev.h>

struct data_t {
    u32 pid;
    u64 rwflag;
    u64 len;
};

BPF_PERF_OUTPUT(events);

// output
int trace_req_completion(struct pt_regs *ctx, struct request *req)
{
    struct data_t data = {};

    id = bpf_get_current_pid_tgid() >> 32;
    tgid = id & (1 <<< 32);
    if (tgid != ##TGID##) {
        return 0;
    }
    data.pid = id >> 32;
    data.len = req->__data_len;

/*
 * The following deals with a kernel version change (in mainline 4.7, although
 * it may be backported to earlier kernels) with how block request write flags
 * are tested. We handle both pre- and post-change versions here. Please avoid
 * kernel version tests like this as much as possible: they inflate the code,
 * test, and maintenance burden.
 */
#ifdef REQ_WRITE
    data.rwflag = !!(req->cmd_flags & REQ_WRITE);
#elif defined(REQ_OP_SHIFT)
    data.rwflag = !!((req->cmd_flags >> REQ_OP_SHIFT) == REQ_OP_WRITE);
#else
    data.rwflag = !!((req->cmd_flags & REQ_OP_MASK) == REQ_OP_WRITE);
#endif

    events.perf_submit(ctx, &data, sizeof(data));
    return 0;
}
#include <uapi/linux/ptrace.h>
#include <linux/blkdev.h>

struct data_t {
    u32 pid;
    u64 rwflag;
    u64 len;
};

struct val_t {
    u32 pid;
};

BPF_HASH(infobyreq, struct request *, struct val_t);
BPF_PERF_OUTPUT(events);

int trace_pid_start(struct pt_regs *ctx, struct request *req)
{
    struct val_t val = {};
    u64 ts;

    u64 id = bpf_get_current_pid_tgid();
    u32 pid = id & (((u64)1 << 32) - 1);
    u32 tgid = id >> 32;
    if (tgid != ##TGID##) {
        return 0;
    }
    val.pid = pid;
    infobyreq.update(&req, &val);
    return 0;
}

// output
int trace_req_completion(struct pt_regs *ctx, struct request *req)
{
    struct data_t data = {};
    struct val_t *valp;

    valp = infobyreq.lookup(&req);
    if (valp == 0) return 0;
    data.pid = valp->pid;
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
    infobyreq.delete(&req);
    return 0;
}
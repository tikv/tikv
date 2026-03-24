# TiKV Resource Metering: Unified Read Pool 与 Scheduler Pool 现状分析

## 1. 背景

这份说明面向 TiKV / PD 开发者，目标是把目前 `unified_read` 和 `scheduler`
两个线程池的 CPU 统计现状讲清楚，并说明下一步值得做什么实验。

本文只讨论下面这几类指标：

| 指标 | 含义 | 可以把它理解成什么 |
| --- | --- | --- |
| `tikv_thread_cpu_seconds_total` | OS 线程级 CPU 计数 | 线程真的用了多少 CPU，最接近真值 |
| `tikv_raftstore_cpu_pool_usage{source="store_level"}` | store heartbeat 按线程名前缀汇总的 CPU | 站在 store 角度看，这个线程池一共用了多少 CPU |
| `tikv_raftstore_cpu_pool_usage{source="metering_total"}` | recorder 在这个线程池上看见的 CPU 总量 | recorder 真正采样到多少 CPU |
| `tikv_raftstore_cpu_pool_usage{source="tag_present_total"}` | recorder 看见 CPU 时，采样点上 tag 存在的部分 | 这部分 CPU 有机会被归到具体请求 |
| `tikv_raftstore_cpu_pool_usage{source="region_sum"}` | 最后成功归到本 store、本 region 的部分 | 真正进入 region 统计的 CPU |
| `tikv_raftstore_cpu_pool_usage{source="tag_absent_untracked"}` | recorder 看见 CPU 时，采样点上没有 tag 的部分 | 这部分 CPU 当前无法归到 region |

对 scheduler，我们又额外加了两组临时观测：

| 指标 | 含义 |
| --- | --- |
| `tikv_resource_metering_scheduler_poll_state_sample_total` | 看采样点数量分布 |
| `tikv_resource_metering_scheduler_poll_state_cpu_millis_total` | 看 CPU 时间分布 |

其中 `state` 有 3 个取值：

| state | 含义 |
| --- | --- |
| `tag_present` | 线程正在 poll 一个 tracked task，而且 tag 已挂上 |
| `task_poll_tag_absent` | 线程正在 poll 一个 tracked task，但此刻没有 tag |
| `outside_task_poll` | 线程当前不在 tracked task poll 里 |

这 3 个状态的意义非常直接：

1. `tag_present` 高，说明 CPU 主要发生在已经挂上 tag 的任务主体里。
2. `task_poll_tag_absent` 高，说明 CPU 主要发生在任务主体里，但 tag 覆盖范围太窄。
3. `outside_task_poll` 高，说明 CPU 主要发生在任务主体之外，更像执行器 / wrapper / runtime 开销。

## 2. 观察方法

### 2.1 监控入口

- Grafana: `http://10.2.12.57:30019/d/RDVQiEzZz/cluster-tikv-details?orgId=1`
- 代理: `http://192.168.8.141:7890`

### 2.2 解释顺序

理解这两类线程池时，建议按下面顺序看：

1. `thread truth` 对比 `store_level`
2. `store_level` 对比 `metering_total`
3. `metering_total` 对比 `tag_present_total`
4. `tag_present_total` 对比 `region_sum`

这四步分别回答四个问题：

1. 线程真值和 store 级汇总是不是对得上？
2. recorder 有没有把线程池 CPU 大体看全？
3. recorder 看到的 CPU 里，有多少在采样点上确实挂着 tag？
4. 挂着 tag 的 CPU 里，又有多少最终成功落到本 store / 本 region？

## 3. Scheduler Pool 当前现状

### 3.1 当前观察窗口

- 观察时间: `2026-03-19 21:27 CST`
- 负载开始时间: `2026-03-19 21:20 CST`
- 负载类型: `update index`

### 3.2 集群汇总

#### 最近 5 分钟

| 项目 | 数值 |
| --- | --- |
| `sched_.*` 线程真值 | `4.44 cores` |
| `store_level` | `4.38 cores` |
| `metering_total` | `3.31 cores` |
| `tag_present_total` | `0.35 cores` |
| `region_sum` | `0.35 cores` |
| `tag_absent_untracked` | `2.96 cores` |
| `tagged_other_store_untracked` | `0` |
| `tagged_non_region_untracked` | `0` |
| `orphan_untracked` | `0` |

换成比例更容易看：

| 对比 | 比例 |
| --- | --- |
| `store_level / thread_truth` | `98.7%` |
| `metering_total / store_level` | `75.7%` |
| `tag_present_total / store_level` | `8.1%` |
| `region_sum / store_level` | `8.1%` |
| `tag_absent_untracked / store_level` | `67.6%` |

#### 从 21:20 到当前整个窗口

| 项目 | 数值 |
| --- | --- |
| `sched_.*` 平均线程真值 | `4.52 cores` |
| `store_level` | `4.59 cores` |
| `metering_total` | `3.53 cores` |
| `tag_present_total` | `0.43 cores` |
| `region_sum` | `0.43 cores` |
| `tag_absent_untracked` | `3.11 cores` |

换成比例：

| 对比 | 比例 |
| --- | --- |
| `store_level / thread_truth` | `101.5%` |
| `metering_total / store_level` | `76.9%` |
| `region_sum / store_level` | `9.3%` |
| `tag_absent_untracked / store_level` | `67.6%` |

这个窗口和最近 5 分钟高度一致，说明现在看到的比例不是瞬时抖动，而是稳定形态。

### 3.3 新三桶观测

#### CPU 时间比例

最近 5 分钟：

| state | 比例 |
| --- | --- |
| `outside_task_poll` | `68.9%` |
| `tag_present` | `28.2%` |
| `task_poll_tag_absent` | `2.8%` |

从 21:20 到当前整个窗口：

| state | 比例 |
| --- | --- |
| `outside_task_poll` | `68.3%` |
| `tag_present` | `28.9%` |
| `task_poll_tag_absent` | `2.8%` |

#### 采样点比例

最近 5 分钟：

| state | 比例 |
| --- | --- |
| `outside_task_poll` | `87.5%` |
| `tag_present` | `11.3%` |
| `task_poll_tag_absent` | `1.2%` |

从 21:20 到当前整个窗口：

| state | 比例 |
| --- | --- |
| `outside_task_poll` | `86.8%` |
| `tag_present` | `11.9%` |
| `task_poll_tag_absent` | `1.2%` |

### 3.4 线程分布

最近 5 分钟真正活跃的线程名：

| 线程名 | CPU |
| --- | --- |
| `sched_pool_0` | `1.1087 cores` |
| `sched_pool_1` | `1.1095 cores` |
| `sched_pool_2` | `1.1071 cores` |
| `sched_pool_3` | `1.1052 cores` |
| `sched_high_0` | `0.0043 cores` |
| `sched_high_1` | `0.0043 cores` |
| `sched_pri_*` | `0` |

这说明当前活跃的几乎全是普通 scheduler worker，`priority pool` 没有真正参与。

### 3.5 对 scheduler 的结论

当前 scheduler 的事实已经比较清楚：

1. `store_level` 和线程真值基本对得上，说明 store 级线程汇总口径没有大问题。
2. `metering_total / store_level` 只有大约 `76%`，说明 recorder 并没有把 scheduler thread CPU 完整看成 region 可归因的那套 CPU。
3. `tag_present_total = region_sum`，并且 `other_store / non_region / orphan = 0`，说明一旦 CPU 进入了 tagged scheduler poll，后面的 store / region 过滤基本没有再丢。
4. 真正的大头是 `outside_task_poll`，不是 `task_poll_tag_absent`。

这句话换成通俗一点的表述就是：

> 现在 scheduler 的主要差距，不是“任务主体里忘了挂 tag”，而是“很多 CPU 本来就发生在任务主体之外”。

也就是说，继续在 `scheduler.rs` 里补零散 `.in_resource_metering_tag(...)`，收益会很有限。
因为当前真正大的那一块 CPU，本来就不在这些业务 future 的主体里。

## 4. Unified Read Pool 当前现状

### 4.1 当前 update index 负载下的现状

同样看 `2026-03-19 21:20` 之后这轮 `update index` 负载。

#### 最近 5 分钟

| 项目 | 数值 |
| --- | --- |
| `unified_read_pool_*` 线程真值 | `1.28 cores` |
| `store_level` | `1.14 cores` |
| `metering_total` | `1.16 cores` |
| `tag_present_total` | `0` |
| `region_sum` | `0` |
| `tag_absent_untracked` | `1.16 cores` |

#### 从 21:20 到当前整个窗口

| 项目 | 数值 |
| --- | --- |
| `unified_read_pool_*` 平均线程真值 | `1.21 cores` |
| `store_level` | `1.11 cores` |
| `metering_total` | `1.14 cores` |
| `tag_present_total` | `0.0007 cores` |
| `region_sum` | `0.0007 cores` |
| `tag_absent_untracked` | `1.14 cores` |

线程分布很平均：

| 线程名 | CPU |
| --- | --- |
| `unified_read_pool_0` | `0.2135 cores` |
| `unified_read_pool_1` | `0.2140 cores` |
| `unified_read_pool_2` | `0.2144 cores` |
| `unified_read_pool_3` | `0.2140 cores` |
| `unified_read_pool_4` | `0.2142 cores` |
| `unified_read_pool_5` | `0.2143 cores` |

### 4.2 这组数据该怎么理解

这组数据很容易被误读。

如果只看当前这轮 `update index`，会得到一个极端结论：unified read pool 当前几乎全是
`tag_absent_untracked`，`region_sum` 接近 0。

但这个结论不能直接外推成“unified read 的长期归因质量很差”，原因是我们之前已经看过两轮专门的读负载：

#### 读负载窗口 1

- 观察时间: `2026-03-19 11:04 CST`

| 项目 | 数值 |
| --- | --- |
| 线程真值 | `1.76 cores` |
| `store_level` | `1.62 cores` |
| `region_sum` | `1.338 cores` |
| `region_sum / store_level` | `82.6%` |
| `store_level / thread_truth` | `92.0%` |

#### 读负载窗口 2

- 观察时间: `2026-03-19 12:56 CST`

| 项目 | 数值 |
| --- | --- |
| 线程真值 | `0.92 cores` |
| `store_level` | `0.83 cores` |
| `region_sum` | `0.714 cores` |
| `region_sum / store_level` | `86.1%` |
| `store_level / thread_truth` | `90.2%` |

### 4.3 对 unified read 的结论

所以 unified read 目前要分两种情况看：

1. **专门的读压测**
   - `region_sum / store_level` 大约在 `82% ~ 86%`
   - 这说明 unified read 在“真正的读请求主路径”上，归因覆盖已经比较好

2. **当前这轮 update index**
   - unified read pool 仍然有大约 `1.1 ~ 1.2 cores` 的 CPU
   - 但几乎全落在 `tag_absent_untracked`
   - 这说明当前负载触发的 unified read CPU，并不像“标准的带请求 tag 的读任务”

通俗一点说：

> 对 unified read，问题不是“长期都不准”，而是“不同 workload 触发的是不同类型的 CPU”。

专门的读压测下，它是准的；当前这轮 update index 下，它看到的是另一类更偏后台或更偏框架的 CPU。

## 5. 统一结论

把两个线程池放在一起看，当前最可靠的结论是：

### 5.1 Scheduler

- 主矛盾已经很清楚，在 `outside_task_poll`
- 不是 `tagged_other_store`
- 不是 `tagged_non_region`
- 不是 `orphan`
- 也不是“任务主体里还漏了很多 tag”

### 5.2 Unified Read

- 在专门读压测下，归因已经明显优于 scheduler
- 在当前 update index 负载下，现状不代表 unified read 的长期主路径，只能说明这轮 workload 触发的 unified read CPU 大部分不带 tag

### 5.3 这意味着什么

如果目标是“把 scheduler pool 的 region CPU 再往 thread truth 靠”，继续补业务路径上的
`.in_resource_metering_tag(...)` 已经不是最有效的方向了。

下一步真正值得验证的，是：

> 是否要把 scheduler 执行器本身那层 CPU，也算到请求头上。

## 6. Scheduler-Only Outer Task Attribution 实验设计

### 6.1 实验目标

验证下面这个判断：

> 如果把 scheduler 线程在 task handling / outer wrapper 这层的 CPU 也记到当前请求，
> `scheduler.region_sum` 会明显向 `scheduler.store_level` 靠近。

### 6.2 设计原则

这个实验必须满足两个要求：

1. **先限定在 scheduler 范围**
   - 不要一上来改 unified read
   - 不要一上来改所有 Yatp 使用者

2. **保留回退能力**
   - 最好加开关
   - 方便 A/B 对比

### 6.3 建议的实验实现

#### 步骤 1: 在 `SchedPool::spawn()` 这一侧传入 outer tag

当前 scheduler 任务已经有 request context，所以可以在提交到 scheduler pool 时，把
`ResourceMeteringTag` 一并带进 task metadata。

涉及位置大致是：

- `src/storage/txn/scheduler.rs`
- `src/storage/txn/sched_pool.rs`

#### 步骤 2: 在 scheduler pool 的任务执行边界 attach outer tag

关键不是再包一层 `future.in_resource_metering_tag(...)`。

因为那样仍然只在 `Future::poll` 期间 attach，无法覆盖我们现在看到的大头
`outside_task_poll`。

真正要实验的是：

- 在 scheduler task 被 Yatp runner 真正处理之前 attach
- 覆盖 `handle(task)` 这整个 task handling 边界

实现上可以做成“scheduler-only 的 opt-in path”：

1. 只对 `SchedPool` 提交的任务设置一个特殊 marker
2. `YatpPoolRunner::handle()` 识别到这个 marker 时，再 attach outer tag
3. 其它线程池、其它 Yatp 使用者不变

这样虽然技术上会碰到共享的 `yatp_pool` 代码，但行为上仍然是 scheduler-only。

#### 步骤 3: outer experiment 打开时，关闭内层 scheduler attach

这一点很重要。

当前 resource metering 的 nested attach 在 debug 下会 assert。
所以 outer task attribution 实验打开时，scheduler 内层这些
`.in_resource_metering_tag(...)` 需要同时切到“只保留 outer attach”的模式：

- `execute()`
- `fail_fast_or_check_deadline()`
- `try_to_wake_up()` error path
- 其它 scheduler pool 入口

否则会出现双层 attach 语义冲突。

### 6.4 实验的判定标准

实验成功与否，不要只看一个数，建议同时看下面几组：

| 指标 | 期望变化 |
| --- | --- |
| `scheduler.outside_task_poll` | 明显下降 |
| `scheduler.tag_present_total` | 明显上升 |
| `scheduler.region_sum` | 明显上升 |
| `scheduler.tag_absent_untracked` | 明显下降 |
| `scheduler.store_level / thread_truth` | 基本不变 |

如果实验后：

- `outside_task_poll` 大幅下降
- `region_sum` 明显靠近 `store_level`

那就说明“执行器税”确实是 scheduler 主差距。

## 7. 这个实验会有什么影响

### 7.1 正面影响

- `scheduler.region_sum` 会更接近 `scheduler.store_level`
- 当前看不清的 scheduler 执行器开销，会被纳入请求归因
- 对写请求相关 region 的 CPU 画像会更完整

### 7.2 负面影响

这不是“白拿精度”，它同时会改变语义。

#### 影响 1: 请求 CPU 会变胖

实验一旦成立，很多以前算在“线程池公共开销”里的 CPU，会开始记到请求上。
所以：

- region CPU 会上升
- TopN / hotspot read or write 的 CPU 排名可能变化
- 和旧版本图不能直接横向比较

#### 影响 2: raw KV modify 也会一起变

raw modify / atomic command 复用同一个 scheduler pool。
所以这个实验不是只影响 txn write，也会连带改变 raw 命令的 CPU 归因。

#### 影响 3: 共享执行器税会被“分配”给当前请求

这是最根本的语义变化。

现在的做法是：

- 只有业务 future 自己在跑时，才算到请求上

outer task attribution 的做法是：

- 任务被执行器处理时，那一层公共开销也算到当前请求上

它的好处是更接近“线程池总 CPU”；
它的代价是“公共税”不再单独存在，而是被分摊给请求。

#### 影响 4: 如果以后想对 unified read 做同样的事情，必须单独评估

当前 unified read 在专门读压测下已经比较接近真值。
所以 scheduler 上成立的实验，不代表应该直接复制到 unified read。

换句话说：

> scheduler 值得做 outer attribution 实验，
> unified read 当前还不值得直接跟进。

## 8. 建议的下一步

按优先级排序，建议是：

1. 先基于当前 scheduler-only 观测，确认是否要做 outer task attribution 实验
2. 如果做实验，必须带开关，保留 A/B 对比
3. 先只做 scheduler，不碰 unified read
4. 如果实验后 `region_sum` 能稳定逼近 `store_level`，再决定是否长期保留这个语义

## 9. 一句话结论

当前的真实现状可以概括成两句话：

1. `scheduler` 的主差距已经不在业务路径漏 tag，而在任务主体之外的执行器 CPU。
2. `unified_read` 在专门读压测下已经明显更准；当前 update index 下看到的“几乎全 absent”，不代表 unified read 长期主路径都不准。

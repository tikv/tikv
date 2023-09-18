// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath] called by Fsm on_ready_compute_hash
use std::{borrow::Cow, marker::PhantomData, mem, ops::Deref};

use engine_traits::{CfName, KvEngine};
use kvproto::{
    metapb::{Region, RegionEpoch},
    pdpb::CheckPolicy,
    raft_cmdpb::{ComputeHashRequest, RaftCmdRequest},
    raft_serverpb::RaftMessage,
};
use protobuf::Message;
use raft::eraftpb;
use tikv_util::box_try;

use super::{split_observer::SplitObserver, *};
use crate::store::BucketRange;

/// A handle for coprocessor to schedule some command back to raftstore.
pub trait StoreHandle: Clone + Send {
    fn update_approximate_size(&self, region_id: u64, size: u64);
    fn update_approximate_keys(&self, region_id: u64, keys: u64);
    fn ask_split(
        &self,
        region_id: u64,
        region_epoch: RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        source: Cow<'static, str>,
    );
    fn refresh_region_buckets(
        &self,
        region_id: u64,
        region_epoch: RegionEpoch,
        buckets: Vec<Bucket>,
        bucket_ranges: Option<Vec<BucketRange>>,
    );
    fn update_compute_hash_result(
        &self,
        region_id: u64,
        index: u64,
        context: Vec<u8>,
        hash: Vec<u8>,
    );
}

#[derive(Clone, Debug, PartialEq)]
pub enum SchedTask {
    UpdateApproximateSize {
        region_id: u64,
        size: u64,
    },
    UpdateApproximateKeys {
        region_id: u64,
        keys: u64,
    },
    AskSplit {
        region_id: u64,
        region_epoch: RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        source: Cow<'static, str>,
    },
    RefreshRegionBuckets {
        region_id: u64,
        region_epoch: RegionEpoch,
        buckets: Vec<Bucket>,
        bucket_ranges: Option<Vec<BucketRange>>,
    },
    UpdateComputeHashResult {
        region_id: u64,
        index: u64,
        hash: Vec<u8>,
        context: Vec<u8>,
    },
}

impl StoreHandle for std::sync::mpsc::SyncSender<SchedTask> {
    fn update_approximate_size(&self, region_id: u64, size: u64) {
        let _ = self.try_send(SchedTask::UpdateApproximateSize { region_id, size });
    }

    fn update_approximate_keys(&self, region_id: u64, keys: u64) {
        let _ = self.try_send(SchedTask::UpdateApproximateKeys { region_id, keys });
    }

    fn ask_split(
        &self,
        region_id: u64,
        region_epoch: RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        source: Cow<'static, str>,
    ) {
        let _ = self.try_send(SchedTask::AskSplit {
            region_id,
            region_epoch,
            split_keys,
            source,
        });
    }

    fn refresh_region_buckets(
        &self,
        region_id: u64,
        region_epoch: RegionEpoch,
        buckets: Vec<Bucket>,
        bucket_ranges: Option<Vec<BucketRange>>,
    ) {
        let _ = self.try_send(SchedTask::RefreshRegionBuckets {
            region_id,
            region_epoch,
            buckets,
            bucket_ranges,
        });
    }

    fn update_compute_hash_result(
        &self,
        region_id: u64,
        index: u64,
        context: Vec<u8>,
        hash: Vec<u8>,
    ) {
        let _ = self.try_send(SchedTask::UpdateComputeHashResult {
            region_id,
            index,
            context,
            hash,
        });
    }
}

struct Entry<T> {
    priority: u32,
    observer: T,
}

impl<T: Clone> Clone for Entry<T> {
    fn clone(&self) -> Self {
        Self {
            priority: self.priority,
            observer: self.observer.clone(),
        }
    }
}

pub trait ClonableObserver: 'static + Send {
    type Ob: ?Sized + Send;
    fn inner(&self) -> &Self::Ob;
    fn inner_mut(&mut self) -> &mut Self::Ob;
    fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Send>;
}

macro_rules! impl_box_observer {
    ($name:ident, $ob:ident, $wrapper:ident) => {
        pub struct $name(Box<dyn ClonableObserver<Ob = dyn $ob> + Send>);
        impl $name {
            pub fn new<T: 'static + $ob + Clone>(observer: T) -> $name {
                $name(Box::new($wrapper { inner: observer }))
            }
        }
        impl Clone for $name {
            fn clone(&self) -> $name {
                $name((**self).box_clone())
            }
        }
        impl Deref for $name {
            type Target = Box<dyn ClonableObserver<Ob = dyn $ob> + Send>;

            fn deref(&self) -> &Box<dyn ClonableObserver<Ob = dyn $ob> + Send> {
                &self.0
            }
        }

        struct $wrapper<T: $ob + Clone> {
            inner: T,
        }
        impl<T: 'static + $ob + Clone> ClonableObserver for $wrapper<T> {
            type Ob = dyn $ob;
            fn inner(&self) -> &Self::Ob {
                &self.inner as _
            }

            fn inner_mut(&mut self) -> &mut Self::Ob {
                &mut self.inner as _
            }

            fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Send> {
                Box::new($wrapper {
                    inner: self.inner.clone(),
                })
            }
        }
    };
}

// This is the same as impl_box_observer_g except $ob has a typaram
macro_rules! impl_box_observer_g {
    ($name:ident, $ob:ident, $wrapper:ident) => {
        pub struct $name<E: KvEngine>(Box<dyn ClonableObserver<Ob = dyn $ob<E>> + Send>);
        impl<E: KvEngine + 'static + Send> $name<E> {
            pub fn new<T: 'static + $ob<E> + Clone>(observer: T) -> $name<E> {
                $name(Box::new($wrapper {
                    inner: observer,
                    _phantom: PhantomData,
                }))
            }
        }
        impl<E: KvEngine + 'static> Clone for $name<E> {
            fn clone(&self) -> $name<E> {
                $name((**self).box_clone())
            }
        }
        impl<E: KvEngine> Deref for $name<E> {
            type Target = Box<dyn ClonableObserver<Ob = dyn $ob<E>> + Send>;

            fn deref(&self) -> &Box<dyn ClonableObserver<Ob = dyn $ob<E>> + Send> {
                &self.0
            }
        }

        struct $wrapper<E: KvEngine, T: $ob<E> + Clone> {
            inner: T,
            _phantom: PhantomData<E>,
        }
        impl<E: KvEngine + 'static + Send, T: 'static + $ob<E> + Clone> ClonableObserver
            for $wrapper<E, T>
        {
            type Ob = dyn $ob<E>;
            fn inner(&self) -> &Self::Ob {
                &self.inner as _
            }

            fn inner_mut(&mut self) -> &mut Self::Ob {
                &mut self.inner as _
            }

            fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Send> {
                Box::new($wrapper {
                    inner: self.inner.clone(),
                    _phantom: PhantomData,
                })
            }
        }
    };
}

impl_box_observer!(BoxAdminObserver, AdminObserver, WrappedAdminObserver);
impl_box_observer!(BoxQueryObserver, QueryObserver, WrappedQueryObserver);
impl_box_observer!(
    BoxUpdateSafeTsObserver,
    UpdateSafeTsObserver,
    WrappedUpdateSafeTsObserver
);
impl_box_observer!(
    BoxApplySnapshotObserver,
    ApplySnapshotObserver,
    WrappedApplySnapshotObserver
);
impl_box_observer_g!(
    BoxSplitCheckObserver,
    SplitCheckObserver,
    WrappedSplitCheckObserver
);
impl_box_observer!(BoxPdTaskObserver, PdTaskObserver, WrappedPdTaskObserver);
impl_box_observer!(BoxRoleObserver, RoleObserver, WrappedRoleObserver);
impl_box_observer!(
    BoxRegionChangeObserver,
    RegionChangeObserver,
    WrappedRegionChangeObserver
);
impl_box_observer!(
    BoxReadIndexObserver,
    ReadIndexObserver,
    WrappedReadIndexObserver
);
impl_box_observer_g!(BoxCmdObserver, CmdObserver, WrappedCmdObserver);
impl_box_observer_g!(
    BoxConsistencyCheckObserver,
    ConsistencyCheckObserver,
    WrappedConsistencyCheckObserver
);
impl_box_observer!(BoxMessageObserver, MessageObserver, WrappedMessageObserver);

/// Registry contains all registered coprocessors.
#[derive(Clone)]
pub struct Registry<E>
where
    E: KvEngine + 'static,
{
    admin_observers: Vec<Entry<BoxAdminObserver>>,
    query_observers: Vec<Entry<BoxQueryObserver>>,
    apply_snapshot_observers: Vec<Entry<BoxApplySnapshotObserver>>,
    split_check_observers: Vec<Entry<BoxSplitCheckObserver<E>>>,
    consistency_check_observers: Vec<Entry<BoxConsistencyCheckObserver<E>>>,
    role_observers: Vec<Entry<BoxRoleObserver>>,
    region_change_observers: Vec<Entry<BoxRegionChangeObserver>>,
    cmd_observers: Vec<Entry<BoxCmdObserver<E>>>,
    read_index_observers: Vec<Entry<BoxReadIndexObserver>>,
    pd_task_observers: Vec<Entry<BoxPdTaskObserver>>,
    update_safe_ts_observers: Vec<Entry<BoxUpdateSafeTsObserver>>,
    message_observers: Vec<Entry<BoxMessageObserver>>,
    // TODO: add endpoint
}

impl<E: KvEngine> Default for Registry<E> {
    fn default() -> Registry<E> {
        Registry {
            admin_observers: Default::default(),
            query_observers: Default::default(),
            apply_snapshot_observers: Default::default(),
            split_check_observers: Default::default(),
            consistency_check_observers: Default::default(),
            role_observers: Default::default(),
            region_change_observers: Default::default(),
            cmd_observers: Default::default(),
            read_index_observers: Default::default(),
            pd_task_observers: Default::default(),
            update_safe_ts_observers: Default::default(),
            message_observers: Default::default(),
        }
    }
}

macro_rules! push {
    ($p:expr, $t:ident, $vec:expr) => {
        $t.inner().start();
        let e = Entry {
            priority: $p,
            observer: $t,
        };
        let vec = &mut $vec;
        vec.push(e);
        vec.sort_by(|l, r| l.priority.cmp(&r.priority));
    };
}

impl<E: KvEngine> Registry<E> {
    pub fn register_admin_observer(&mut self, priority: u32, ao: BoxAdminObserver) {
        push!(priority, ao, self.admin_observers);
    }

    pub fn register_query_observer(&mut self, priority: u32, qo: BoxQueryObserver) {
        push!(priority, qo, self.query_observers);
    }

    pub fn register_apply_snapshot_observer(
        &mut self,
        priority: u32,
        aso: BoxApplySnapshotObserver,
    ) {
        push!(priority, aso, self.apply_snapshot_observers);
    }

    pub fn register_split_check_observer(&mut self, priority: u32, sco: BoxSplitCheckObserver<E>) {
        push!(priority, sco, self.split_check_observers);
    }

    pub fn register_consistency_check_observer(
        &mut self,
        priority: u32,
        cco: BoxConsistencyCheckObserver<E>,
    ) {
        push!(priority, cco, self.consistency_check_observers);
    }

    pub fn register_pd_task_observer(&mut self, priority: u32, ro: BoxPdTaskObserver) {
        push!(priority, ro, self.pd_task_observers);
    }

    pub fn register_role_observer(&mut self, priority: u32, ro: BoxRoleObserver) {
        push!(priority, ro, self.role_observers);
    }

    pub fn register_region_change_observer(&mut self, priority: u32, rlo: BoxRegionChangeObserver) {
        push!(priority, rlo, self.region_change_observers);
    }

    pub fn register_cmd_observer(&mut self, priority: u32, rlo: BoxCmdObserver<E>) {
        push!(priority, rlo, self.cmd_observers);
    }

    pub fn register_read_index_observer(&mut self, priority: u32, rio: BoxReadIndexObserver) {
        push!(priority, rio, self.read_index_observers);
    }
    pub fn register_update_safe_ts_observer(&mut self, priority: u32, qo: BoxUpdateSafeTsObserver) {
        push!(priority, qo, self.update_safe_ts_observers);
    }

    pub fn register_message_observer(&mut self, priority: u32, qo: BoxMessageObserver) {
        push!(priority, qo, self.message_observers);
    }
}

/// A macro that loops over all observers and returns early when error is found
/// or bypass is set. `try_loop_ob` is expected to be used for hook that returns
/// a `Result`.
macro_rules! try_loop_ob {
    ($r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {
        loop_ob!(_imp _res, $r, $obs, $hook, $($args)*)
    };
}

/// A macro that loops over all observers and returns early when bypass is set.
///
/// Using a macro so we don't need to write tests for every observers.
macro_rules! loop_ob {
    // Execute a hook, return early if error is found.
    (_exec _res, $o:expr, $hook:ident, $ctx:expr, $($args:tt)*) => {
        $o.inner().$hook($ctx, $($args)*)?
    };
    // Execute a hook.
    (_exec _tup, $o:expr, $hook:ident, $ctx:expr, $($args:tt)*) => {
        $o.inner().$hook($ctx, $($args)*)
    };
    // When the try loop finishes successfully, the value to be returned.
    (_done _res) => {
        Ok(())
    };
    // When the loop finishes successfully, the value to be returned.
    (_done _tup) => {{}};
    // Actual implementation of the for loop.
    (_imp $res_type:tt, $r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {{
        let mut ctx = ObserverContext::new($r);
        for o in $obs {
            loop_ob!(_exec $res_type, o.observer, $hook, &mut ctx, $($args)*);
            if ctx.bypass {
                break;
            }
        }
        loop_ob!(_done $res_type)
    }};
    // Loop over all observers and return early when bypass is set.
    // This macro is expected to be used for hook that returns `()`.
    ($r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {
        loop_ob!(_imp _tup, $r, $obs, $hook, $($args)*)
    };
}

/// Admin and invoke all coprocessors.
#[derive(Clone)]
pub struct CoprocessorHost<E>
where
    E: KvEngine + 'static,
{
    pub registry: Registry<E>,
    pub cfg: Config,
}

impl<E: KvEngine> Default for CoprocessorHost<E>
where
    E: 'static,
{
    fn default() -> Self {
        CoprocessorHost {
            registry: Default::default(),
            cfg: Default::default(),
        }
    }
}

impl<E: KvEngine> CoprocessorHost<E> {
    pub fn new<C: StoreHandle + Clone + Send + 'static>(ch: C, cfg: Config) -> CoprocessorHost<E> {
        // TODO load coprocessors from configuration
        let mut registry = Registry::default();
        registry.register_split_check_observer(
            200,
            BoxSplitCheckObserver::new(SizeCheckObserver::new(ch.clone())),
        );
        registry.register_split_check_observer(
            200,
            BoxSplitCheckObserver::new(KeysCheckObserver::new(ch)),
        );
        registry.register_split_check_observer(100, BoxSplitCheckObserver::new(HalfCheckObserver));
        registry.register_split_check_observer(400, BoxSplitCheckObserver::new(TableCheckObserver));
        registry.register_admin_observer(100, BoxAdminObserver::new(SplitObserver));
        CoprocessorHost { registry, cfg }
    }

    pub fn on_empty_cmd(&self, region: &Region, index: u64, term: u64) {
        loop_ob!(
            region,
            &self.registry.query_observers,
            on_empty_cmd,
            index,
            term,
        );
    }

    /// Call all propose hooks until bypass is set to true.
    pub fn pre_propose(&self, region: &Region, req: &mut RaftCmdRequest) -> Result<()> {
        if !req.has_admin_request() {
            let query = req.mut_requests();
            let mut vec_query = mem::take(query).into();
            let result = try_loop_ob!(
                region,
                &self.registry.query_observers,
                pre_propose_query,
                &mut vec_query,
            );
            *query = vec_query.into();
            result
        } else {
            let admin = req.mut_admin_request();
            try_loop_ob!(
                region,
                &self.registry.admin_observers,
                pre_propose_admin,
                admin
            )
        }
    }

    /// Call all pre apply hook until bypass is set to true.
    pub fn pre_apply(&self, region: &Region, req: &RaftCmdRequest) {
        if !req.has_admin_request() {
            let query = req.get_requests();
            loop_ob!(
                region,
                &self.registry.query_observers,
                pre_apply_query,
                query,
            );
        } else {
            let admin = req.get_admin_request();
            loop_ob!(
                region,
                &self.registry.admin_observers,
                pre_apply_admin,
                admin
            );
        }
    }

    pub fn post_apply(&self, region: &Region, cmd: &Cmd) {
        if !cmd.response.has_admin_response() {
            loop_ob!(
                region,
                &self.registry.query_observers,
                post_apply_query,
                cmd,
            );
        } else {
            let admin = cmd.response.get_admin_response();
            loop_ob!(
                region,
                &self.registry.admin_observers,
                post_apply_admin,
                admin
            );
        }
    }

    // (index, term) is for the applying entry.
    pub fn pre_exec(&self, region: &Region, cmd: &RaftCmdRequest, index: u64, term: u64) -> bool {
        let mut ctx = ObserverContext::new(region);
        if !cmd.has_admin_request() {
            let query = cmd.get_requests();
            for observer in &self.registry.query_observers {
                let observer = observer.observer.inner();
                if observer.pre_exec_query(&mut ctx, query, index, term) {
                    return true;
                }
            }
            false
        } else {
            let admin = cmd.get_admin_request();
            for observer in &self.registry.admin_observers {
                let observer = observer.observer.inner();
                if observer.pre_exec_admin(&mut ctx, admin, index, term) {
                    return true;
                }
            }
            false
        }
    }

    /// `post_exec` should be called immediately after we executed one raft
    /// command. It notifies observers side effects of this command before
    /// execution of the next command, including req/resp, apply state,
    /// modified region state, etc. Return true observers think a
    /// persistence is necessary.
    pub fn post_exec(
        &self,
        region: &Region,
        cmd: &Cmd,
        apply_state: &RaftApplyState,
        region_state: &RegionState,
        apply_ctx: &mut ApplyCtxInfo<'_>,
    ) -> bool {
        let mut ctx = ObserverContext::new(region);
        if !cmd.response.has_admin_response() {
            for observer in &self.registry.query_observers {
                let observer = observer.observer.inner();
                if observer.post_exec_query(&mut ctx, cmd, apply_state, region_state, apply_ctx) {
                    return true;
                }
            }
            false
        } else {
            for observer in &self.registry.admin_observers {
                let observer = observer.observer.inner();
                if observer.post_exec_admin(&mut ctx, cmd, apply_state, region_state, apply_ctx) {
                    return true;
                }
            }
            false
        }
    }

    pub fn post_apply_plain_kvs_from_snapshot(
        &self,
        region: &Region,
        cf: CfName,
        kv_pairs: &[(Vec<u8>, Vec<u8>)],
    ) {
        loop_ob!(
            region,
            &self.registry.apply_snapshot_observers,
            apply_plain_kvs,
            cf,
            kv_pairs
        );
    }

    pub fn post_apply_sst_from_snapshot(&self, region: &Region, cf: CfName, path: &str) {
        loop_ob!(
            region,
            &self.registry.apply_snapshot_observers,
            apply_sst,
            cf,
            path
        );
    }

    pub fn should_pre_apply_snapshot(&self) -> bool {
        for observer in &self.registry.apply_snapshot_observers {
            let observer = observer.observer.inner();
            if observer.should_pre_apply_snapshot() {
                return true;
            }
        }
        false
    }

    pub fn pre_apply_snapshot(
        &self,
        region: &Region,
        peer_id: u64,
        snap_key: &crate::store::SnapKey,
        snap: Option<&crate::store::Snapshot>,
    ) {
        loop_ob!(
            region,
            &self.registry.apply_snapshot_observers,
            pre_apply_snapshot,
            peer_id,
            snap_key,
            snap,
        );
    }

    pub fn post_apply_snapshot(
        &self,
        region: &Region,
        peer_id: u64,
        snap_key: &crate::store::SnapKey,
        snap: Option<&crate::store::Snapshot>,
    ) {
        let mut ctx = ObserverContext::new(region);
        for observer in &self.registry.apply_snapshot_observers {
            let observer = observer.observer.inner();
            observer.post_apply_snapshot(&mut ctx, peer_id, snap_key, snap);
        }
    }

    pub fn cancel_apply_snapshot(&self, region_id: u64, peer_id: u64) {
        for observer in &self.registry.apply_snapshot_observers {
            let observer = observer.observer.inner();
            observer.cancel_apply_snapshot(region_id, peer_id);
        }
    }

    pub fn new_split_checker_host<'a>(
        &'a self,
        region: &Region,
        engine: &E,
        auto_split: bool,
        policy: CheckPolicy,
    ) -> SplitCheckerHost<'a, E> {
        let mut host = SplitCheckerHost::new(auto_split, &self.cfg);
        loop_ob!(
            region,
            &self.registry.split_check_observers,
            add_checker,
            &mut host,
            engine,
            policy
        );
        host
    }

    pub fn on_prepropose_compute_hash(&self, req: &mut ComputeHashRequest) {
        for observer in &self.registry.consistency_check_observers {
            let observer = observer.observer.inner();
            if observer.update_context(req.mut_context()) {
                break;
            }
        }
    }

    pub fn on_compute_hash(
        &self,
        region: &Region,
        context: &[u8],
        snap: E::Snapshot,
    ) -> Result<Vec<(Vec<u8>, u32)>> {
        let mut hashes = Vec::new();
        let (mut reader, context_len) = (context, context.len());
        for observer in &self.registry.consistency_check_observers {
            let observer = observer.observer.inner();
            let old_len = reader.len();
            let hash = match box_try!(observer.compute_hash(region, &mut reader, &snap)) {
                Some(hash) => hash,
                None => break,
            };
            let new_len = reader.len();
            let ctx = context[context_len - old_len..context_len - new_len].to_vec();
            hashes.push((ctx, hash));
        }
        Ok(hashes)
    }

    pub fn on_compute_engine_size(&self) -> Option<StoreSizeInfo> {
        let mut store_size = None;
        for observer in &self.registry.pd_task_observers {
            let observer = observer.observer.inner();
            observer.on_compute_engine_size(&mut store_size);
        }
        store_size
    }

    pub fn on_role_change(&self, region: &Region, role_change: RoleChange) {
        loop_ob!(
            region,
            &self.registry.role_observers,
            on_role_change,
            &role_change
        );
    }

    pub fn on_region_changed(&self, region: &Region, event: RegionChangeEvent, role: StateRole) {
        loop_ob!(
            region,
            &self.registry.region_change_observers,
            on_region_changed,
            event,
            role
        );
    }

    /// `pre_persist` is called we we want to persist data or meta for a region.
    /// For example, in `finish_for` and `commit`,
    /// we will separately call `pre_persist` with is_finished = true/false.
    /// By returning false, we reject this persistence.
    pub fn pre_persist(
        &self,
        region: &Region,
        is_finished: bool,
        cmd: Option<&RaftCmdRequest>,
    ) -> bool {
        let mut ctx = ObserverContext::new(region);
        for observer in &self.registry.region_change_observers {
            let observer = observer.observer.inner();
            if !observer.pre_persist(&mut ctx, is_finished, cmd) {
                return false;
            }
        }
        true
    }

    /// Should be called everytime before we want to write apply state when
    /// applying. Return a bool which indicates whether we can actually do
    /// this write.
    pub fn pre_write_apply_state(&self, region: &Region) -> bool {
        let mut ctx = ObserverContext::new(region);
        for observer in &self.registry.region_change_observers {
            let observer = observer.observer.inner();
            if !observer.pre_write_apply_state(&mut ctx) {
                return false;
            }
        }
        true
    }

    /// Returns false if the message should not be stepped later.
    pub fn on_raft_message(&self, msg: &RaftMessage) -> bool {
        for observer in &self.registry.message_observers {
            let observer = observer.observer.inner();
            if !observer.on_raft_message(msg) {
                return false;
            }
        }
        true
    }

    pub fn on_flush_applied_cmd_batch(
        &self,
        max_level: ObserveLevel,
        mut cmd_batches: Vec<CmdBatch>,
        engine: &E,
    ) {
        // Some observer assert `cmd_batches` is not empty
        if cmd_batches.is_empty() {
            return;
        }
        for batch in &cmd_batches {
            for cmd in &batch.cmds {
                self.post_apply(Region::default_instance(), cmd);
            }
        }
        for observer in &self.registry.cmd_observers {
            let observer = observer.observer.inner();
            observer.on_flush_applied_cmd_batch(max_level, &mut cmd_batches, engine);
        }
    }

    pub fn on_applied_current_term(&self, role: StateRole, region: &Region) {
        if self.registry.cmd_observers.is_empty() {
            return;
        }
        for observer in &self.registry.cmd_observers {
            let observer = observer.observer.inner();
            observer.on_applied_current_term(role, region);
        }
    }

    pub fn on_step_read_index(&self, msg: &mut eraftpb::Message, role: StateRole) {
        for step_ob in &self.registry.read_index_observers {
            step_ob.observer.inner().on_step(msg, role);
        }
    }

    pub fn on_update_safe_ts(&self, region_id: u64, self_safe_ts: u64, leader_safe_ts: u64) {
        if self.registry.query_observers.is_empty() {
            return;
        }
        for observer in &self.registry.update_safe_ts_observers {
            let observer = observer.observer.inner();
            observer.on_update_safe_ts(region_id, self_safe_ts, leader_safe_ts)
        }
    }

    pub fn shutdown(&self) {
        for entry in &self.registry.admin_observers {
            entry.observer.inner().stop();
        }
        for entry in &self.registry.query_observers {
            entry.observer.inner().stop();
        }
        for entry in &self.registry.split_check_observers {
            entry.observer.inner().stop();
        }
        for entry in &self.registry.cmd_observers {
            entry.observer.inner().stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use engine_panic::PanicEngine;
    use kvproto::{
        metapb::Region,
        raft_cmdpb::{AdminRequest, AdminResponse, RaftCmdRequest, RaftCmdResponse, Request},
    };
    use tikv_util::box_err;

    use crate::{
        coprocessor::{dispatcher::BoxUpdateSafeTsObserver, *},
        store::{SnapKey, Snapshot},
    };

    #[derive(Clone, Default)]
    struct TestCoprocessor {
        bypass: Arc<AtomicBool>,
        called: Arc<AtomicUsize>,
        return_err: Arc<AtomicBool>,
    }

    enum ObserverIndex {
        PreProposeAdmin = 1,
        PreApplyAdmin = 2,
        PostApplyAdmin = 3,
        PreProposeQuery = 4,
        PreApplyQuery = 5,
        PostApplyQuery = 6,
        OnRoleChange = 7,
        OnRegionChanged = 8,
        ApplyPlainKvs = 9,
        ApplySst = 10,
        OnFlushAppliedCmdBatch = 13,
        OnEmptyCmd = 14,
        PreExecQuery = 15,
        PreExecAdmin = 16,
        PostExecQuery = 17,
        PostExecAdmin = 18,
        OnComputeEngineSize = 19,
        PreApplySnapshot = 20,
        PostApplySnapshot = 21,
        ShouldPreApplySnapshot = 22,
        OnUpdateSafeTs = 23,
        PrePersist = 24,
        PreWriteApplyState = 25,
        OnRaftMessage = 26,
        CancelApplySnapshot = 27,
    }

    impl Coprocessor for TestCoprocessor {}

    impl AdminObserver for TestCoprocessor {
        fn pre_propose_admin(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: &mut AdminRequest,
        ) -> Result<()> {
            self.called
                .fetch_add(ObserverIndex::PreProposeAdmin as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            if self.return_err.load(Ordering::SeqCst) {
                return Err(box_err!("error"));
            }
            Ok(())
        }

        fn pre_apply_admin(&self, ctx: &mut ObserverContext<'_>, _: &AdminRequest) {
            self.called
                .fetch_add(ObserverIndex::PreApplyAdmin as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn post_apply_admin(&self, ctx: &mut ObserverContext<'_>, _: &AdminResponse) {
            self.called
                .fetch_add(ObserverIndex::PostApplyAdmin as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn pre_exec_admin(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: &AdminRequest,
            _: u64,
            _: u64,
        ) -> bool {
            self.called
                .fetch_add(ObserverIndex::PreExecAdmin as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            false
        }

        fn post_exec_admin(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: &Cmd,
            _: &RaftApplyState,
            _: &RegionState,
            _: &mut ApplyCtxInfo<'_>,
        ) -> bool {
            self.called
                .fetch_add(ObserverIndex::PostExecAdmin as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            false
        }
    }

    impl QueryObserver for TestCoprocessor {
        fn pre_propose_query(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: &mut Vec<Request>,
        ) -> Result<()> {
            self.called
                .fetch_add(ObserverIndex::PreProposeQuery as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            if self.return_err.load(Ordering::SeqCst) {
                return Err(box_err!("error"));
            }
            Ok(())
        }

        fn pre_apply_query(&self, ctx: &mut ObserverContext<'_>, _: &[Request]) {
            self.called
                .fetch_add(ObserverIndex::PreApplyQuery as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn post_apply_query(&self, ctx: &mut ObserverContext<'_>, _: &Cmd) {
            self.called
                .fetch_add(ObserverIndex::PostApplyQuery as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn pre_exec_query(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: &[Request],
            _: u64,
            _: u64,
        ) -> bool {
            self.called
                .fetch_add(ObserverIndex::PreExecQuery as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            false
        }

        fn on_empty_cmd(&self, ctx: &mut ObserverContext<'_>, _index: u64, _term: u64) {
            self.called
                .fetch_add(ObserverIndex::OnEmptyCmd as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn post_exec_query(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: &Cmd,
            _: &RaftApplyState,
            _: &RegionState,
            _: &mut ApplyCtxInfo<'_>,
        ) -> bool {
            self.called
                .fetch_add(ObserverIndex::PostExecQuery as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            false
        }
    }

    impl PdTaskObserver for TestCoprocessor {
        fn on_compute_engine_size(&self, _: &mut Option<StoreSizeInfo>) {
            self.called.fetch_add(
                ObserverIndex::OnComputeEngineSize as usize,
                Ordering::SeqCst,
            );
        }
    }

    impl RoleObserver for TestCoprocessor {
        fn on_role_change(&self, ctx: &mut ObserverContext<'_>, _: &RoleChange) {
            self.called
                .fetch_add(ObserverIndex::OnRoleChange as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl RegionChangeObserver for TestCoprocessor {
        fn on_region_changed(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: RegionChangeEvent,
            _: StateRole,
        ) {
            self.called
                .fetch_add(ObserverIndex::OnRegionChanged as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn pre_persist(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: bool,
            _: Option<&RaftCmdRequest>,
        ) -> bool {
            self.called
                .fetch_add(ObserverIndex::PrePersist as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            true
        }

        fn pre_write_apply_state(&self, ctx: &mut ObserverContext<'_>) -> bool {
            self.called
                .fetch_add(ObserverIndex::PreWriteApplyState as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            true
        }
    }

    impl ApplySnapshotObserver for TestCoprocessor {
        fn apply_plain_kvs(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: CfName,
            _: &[(Vec<u8>, Vec<u8>)],
        ) {
            self.called
                .fetch_add(ObserverIndex::ApplyPlainKvs as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn apply_sst(&self, ctx: &mut ObserverContext<'_>, _: CfName, _: &str) {
            self.called
                .fetch_add(ObserverIndex::ApplySst as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn pre_apply_snapshot(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: u64,
            _: &SnapKey,
            _: Option<&Snapshot>,
        ) {
            self.called
                .fetch_add(ObserverIndex::PreApplySnapshot as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn post_apply_snapshot(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: u64,
            _: &crate::store::SnapKey,
            _: Option<&Snapshot>,
        ) {
            self.called
                .fetch_add(ObserverIndex::PostApplySnapshot as usize, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn should_pre_apply_snapshot(&self) -> bool {
            self.called.fetch_add(
                ObserverIndex::ShouldPreApplySnapshot as usize,
                Ordering::SeqCst,
            );
            false
        }

        fn cancel_apply_snapshot(&self, _: u64, _: u64) {
            self.called.fetch_add(
                ObserverIndex::CancelApplySnapshot as usize,
                Ordering::SeqCst,
            );
        }
    }

    impl CmdObserver<PanicEngine> for TestCoprocessor {
        fn on_flush_applied_cmd_batch(
            &self,
            _: ObserveLevel,
            _: &mut Vec<CmdBatch>,
            _: &PanicEngine,
        ) {
            self.called.fetch_add(
                ObserverIndex::OnFlushAppliedCmdBatch as usize,
                Ordering::SeqCst,
            );
        }
        fn on_applied_current_term(&self, _: StateRole, _: &Region) {}
    }

    impl UpdateSafeTsObserver for TestCoprocessor {
        fn on_update_safe_ts(&self, _: u64, _: u64, _: u64) {
            self.called
                .fetch_add(ObserverIndex::OnUpdateSafeTs as usize, Ordering::SeqCst);
        }
    }

    impl MessageObserver for TestCoprocessor {
        fn on_raft_message(&self, _: &RaftMessage) -> bool {
            self.called
                .fetch_add(ObserverIndex::OnRaftMessage as usize, Ordering::SeqCst);
            true
        }
    }

    macro_rules! assert_all {
        ($target:expr, $expect:expr) => {{
            for (c, e) in ($target).iter().zip($expect) {
                assert_eq!(c.load(Ordering::SeqCst), *e);
            }
        }};
    }

    macro_rules! set_all {
        ($target:expr, $v:expr) => {{
            for v in $target {
                v.store($v, Ordering::SeqCst);
            }
        }};
    }

    #[test]
    fn test_trigger_right_hook() {
        let mut host = CoprocessorHost::<PanicEngine>::default();
        let ob = TestCoprocessor::default();
        host.registry
            .register_admin_observer(1, BoxAdminObserver::new(ob.clone()));
        host.registry
            .register_query_observer(1, BoxQueryObserver::new(ob.clone()));
        host.registry
            .register_apply_snapshot_observer(1, BoxApplySnapshotObserver::new(ob.clone()));
        host.registry
            .register_pd_task_observer(1, BoxPdTaskObserver::new(ob.clone()));
        host.registry
            .register_role_observer(1, BoxRoleObserver::new(ob.clone()));
        host.registry
            .register_region_change_observer(1, BoxRegionChangeObserver::new(ob.clone()));
        host.registry
            .register_cmd_observer(1, BoxCmdObserver::new(ob.clone()));
        host.registry
            .register_update_safe_ts_observer(1, BoxUpdateSafeTsObserver::new(ob.clone()));
        host.registry
            .register_message_observer(1, BoxMessageObserver::new(ob.clone()));

        let mut index: usize = 0;
        let region = Region::default();
        let mut admin_req = RaftCmdRequest::default();
        admin_req.set_admin_request(AdminRequest::default());
        host.pre_propose(&region, &mut admin_req).unwrap();
        index += ObserverIndex::PreProposeAdmin as usize;
        assert_all!([&ob.called], &[index]);
        host.pre_apply(&region, &admin_req);
        index += ObserverIndex::PreApplyAdmin as usize;
        assert_all!([&ob.called], &[index]);
        let mut admin_resp = RaftCmdResponse::default();
        admin_resp.set_admin_response(AdminResponse::default());
        host.post_apply(&region, &Cmd::new(0, 0, admin_req, admin_resp));
        index += ObserverIndex::PostApplyAdmin as usize;
        assert_all!([&ob.called], &[index]);

        let mut query_req = RaftCmdRequest::default();
        query_req.set_requests(vec![Request::default()].into());
        host.pre_propose(&region, &mut query_req).unwrap();
        index += ObserverIndex::PreProposeQuery as usize;
        assert_all!([&ob.called], &[index]);
        index += ObserverIndex::PreApplyQuery as usize;
        host.pre_apply(&region, &query_req);
        assert_all!([&ob.called], &[index]);
        let query_resp = RaftCmdResponse::default();
        host.post_apply(&region, &Cmd::new(0, 0, query_req, query_resp));
        index += ObserverIndex::PostApplyQuery as usize;
        assert_all!([&ob.called], &[index]);

        host.on_role_change(&region, RoleChange::new(StateRole::Leader));
        index += ObserverIndex::OnRoleChange as usize;
        assert_all!([&ob.called], &[index]);

        host.on_region_changed(&region, RegionChangeEvent::Create, StateRole::Follower);
        index += ObserverIndex::OnRegionChanged as usize;
        assert_all!([&ob.called], &[index]);

        host.post_apply_plain_kvs_from_snapshot(&region, "default", &[]);
        index += ObserverIndex::ApplyPlainKvs as usize;
        assert_all!([&ob.called], &[index]);
        host.post_apply_sst_from_snapshot(&region, "default", "");
        index += ObserverIndex::ApplySst as usize;
        assert_all!([&ob.called], &[index]);

        let observe_info = CmdObserveInfo::from_handle(
            ObserveHandle::new(),
            ObserveHandle::new(),
            ObserveHandle::new(),
        );
        let mut cb = CmdBatch::new(&observe_info, 0);
        cb.push(&observe_info, 0, Cmd::default());
        host.on_flush_applied_cmd_batch(cb.level, vec![cb], &PanicEngine);
        index += ObserverIndex::PostApplyQuery as usize;
        index += ObserverIndex::OnFlushAppliedCmdBatch as usize;
        assert_all!([&ob.called], &[index]);

        let mut empty_req = RaftCmdRequest::default();
        empty_req.set_requests(vec![Request::default()].into());
        host.on_empty_cmd(&region, 0, 0);
        index += ObserverIndex::OnEmptyCmd as usize;
        assert_all!([&ob.called], &[index]);

        let mut query_req = RaftCmdRequest::default();
        query_req.set_requests(vec![Request::default()].into());
        host.pre_exec(&region, &query_req, 0, 0);
        index += ObserverIndex::PreExecQuery as usize;
        assert_all!([&ob.called], &[index]);

        let mut admin_req = RaftCmdRequest::default();
        admin_req.set_admin_request(AdminRequest::default());
        host.pre_exec(&region, &admin_req, 0, 0);
        index += ObserverIndex::PreExecAdmin as usize;
        assert_all!([&ob.called], &[index]);

        host.on_compute_engine_size();
        index += ObserverIndex::OnComputeEngineSize as usize;
        assert_all!([&ob.called], &[index]);

        let mut pending_handle_ssts = None;
        let mut delete_ssts = vec![];
        let mut pending_delete_ssts = vec![];
        let mut info = ApplyCtxInfo {
            pending_handle_ssts: &mut pending_handle_ssts,
            pending_delete_ssts: &mut pending_delete_ssts,
            delete_ssts: &mut delete_ssts,
        };
        let apply_state = RaftApplyState::default();
        let region_state = RegionState::default();
        let cmd = Cmd::default();
        host.post_exec(&region, &cmd, &apply_state, &region_state, &mut info);
        index += ObserverIndex::PostExecQuery as usize;
        assert_all!([&ob.called], &[index]);

        let key = SnapKey::new(region.get_id(), 1, 1);
        host.pre_apply_snapshot(&region, 0, &key, None);
        index += ObserverIndex::PreApplySnapshot as usize;
        assert_all!([&ob.called], &[index]);

        host.post_apply_snapshot(&region, 0, &key, None);
        index += ObserverIndex::PostApplySnapshot as usize;
        assert_all!([&ob.called], &[index]);

        host.should_pre_apply_snapshot();
        index += ObserverIndex::ShouldPreApplySnapshot as usize;
        assert_all!([&ob.called], &[index]);

        host.on_update_safe_ts(1, 1, 1);
        index += ObserverIndex::OnUpdateSafeTs as usize;
        assert_all!([&ob.called], &[index]);

        host.pre_write_apply_state(&region);
        index += ObserverIndex::PreWriteApplyState as usize;
        assert_all!([&ob.called], &[index]);

        let msg = RaftMessage::default();
        host.on_raft_message(&msg);
        index += ObserverIndex::OnRaftMessage as usize;
        assert_all!([&ob.called], &[index]);

        host.cancel_apply_snapshot(region.get_id(), 0);
        index += ObserverIndex::CancelApplySnapshot as usize;
        assert_all!([&ob.called], &[index]);
    }

    #[test]
    fn test_order() {
        let mut host = CoprocessorHost::<PanicEngine>::default();

        let ob1 = TestCoprocessor::default();
        host.registry
            .register_admin_observer(3, BoxAdminObserver::new(ob1.clone()));
        host.registry
            .register_query_observer(3, BoxQueryObserver::new(ob1.clone()));
        let ob2 = TestCoprocessor::default();
        host.registry
            .register_admin_observer(2, BoxAdminObserver::new(ob2.clone()));
        host.registry
            .register_query_observer(2, BoxQueryObserver::new(ob2.clone()));

        let region = Region::default();
        let mut admin_req = RaftCmdRequest::default();
        admin_req.set_admin_request(AdminRequest::default());
        let mut admin_resp = RaftCmdResponse::default();
        admin_resp.set_admin_response(AdminResponse::default());
        let query_req = RaftCmdRequest::default();
        let query_resp = RaftCmdResponse::default();

        let cases = vec![(0, admin_req, admin_resp), (3, query_req, query_resp)];

        for (base_score, mut req, resp) in cases {
            set_all!(&[&ob1.return_err, &ob2.return_err], false);
            set_all!(&[&ob1.called, &ob2.called], 0);
            set_all!(&[&ob1.bypass, &ob2.bypass], true);

            host.pre_propose(&region, &mut req).unwrap();

            // less means more.
            assert_all!([&ob1.called, &ob2.called], &[0, base_score + 1]);

            host.pre_apply(&region, &req);
            assert_all!([&ob1.called, &ob2.called], &[0, base_score * 2 + 3]);

            host.post_apply(&region, &Cmd::new(0, 0, req.clone(), resp.clone()));
            assert_all!([&ob1.called, &ob2.called], &[0, base_score * 3 + 6]);

            set_all!(&[&ob2.bypass], false);
            set_all!(&[&ob2.called], 0);

            host.pre_propose(&region, &mut req).unwrap();

            assert_all!(
                [&ob1.called, &ob2.called],
                &[base_score + 1, base_score + 1]
            );

            set_all!(&[&ob1.called, &ob2.called], 0);

            // when return error, following coprocessor should not be run.
            set_all!(&[&ob2.return_err], true);
            host.pre_propose(&region, &mut req).unwrap_err();
            assert_all!([&ob1.called, &ob2.called], &[0, base_score + 1]);
        }
    }
}

#[allow(non_snake_case, non_camel_case_types, non_upper_case_globals)]
pub mod root {
    pub mod DB {
        #[allow(unused_imports)]
        use self::super::super::root;
        pub type ConstRawVoidPtr = *const ::std::os::raw::c_void;
        pub type RawVoidPtr = *mut ::std::os::raw::c_void;
        #[repr(C)]
        #[derive(Debug)]
        pub struct RawCppString {
            _unused: [u8; 0],
        }
        pub type RawCppStringPtr = *mut root::DB::RawCppString;
        #[repr(u8)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum ColumnFamilyType {
            Lock = 0,
            Write = 1,
            Default = 2,
        }
        #[repr(u8)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum FileEncryptionRes {
            Disabled = 0,
            Ok = 1,
            Error = 2,
        }
        #[repr(u8)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum EncryptionMethod {
            Unknown = 0,
            Plaintext = 1,
            Aes128Ctr = 2,
            Aes192Ctr = 3,
            Aes256Ctr = 4,
            SM4Ctr = 5,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct FileEncryptionInfoRaw {
            pub res: root::DB::FileEncryptionRes,
            pub method: root::DB::EncryptionMethod,
            pub key: root::DB::RawCppStringPtr,
            pub iv: root::DB::RawCppStringPtr,
            pub error_msg: root::DB::RawCppStringPtr,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct EngineStoreServerWrap {
            _unused: [u8; 0],
        }
        #[repr(u32)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum SpecialCppPtrType {
            None = 0,
            TupleOfRawCppPtr = 1,
            ArrayOfRawCppPtr = 2,
        }
        #[repr(u32)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum EngineStoreApplyRes {
            None = 0,
            Persist = 1,
            NotFound = 2,
        }
        #[repr(u8)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum WriteCmdType {
            Put = 0,
            Del = 1,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct BaseBuffView {
            pub data: *const ::std::os::raw::c_char,
            pub len: u64,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct RaftCmdHeader {
            pub region_id: u64,
            pub index: u64,
            pub term: u64,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct WriteCmdsView {
            pub keys: *const root::DB::BaseBuffView,
            pub vals: *const root::DB::BaseBuffView,
            pub cmd_types: *const root::DB::WriteCmdType,
            pub cmd_cf: *const root::DB::ColumnFamilyType,
            pub len: u64,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct FsStats {
            pub used_size: u64,
            pub avail_size: u64,
            pub capacity_size: u64,
            pub ok: u8,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct StoreStats {
            pub fs_stats: root::DB::FsStats,
            pub engine_bytes_written: u64,
            pub engine_keys_written: u64,
            pub engine_bytes_read: u64,
            pub engine_keys_read: u64,
        }
        #[repr(u16)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum SSTFormatKind {
            KIND_SST = 0,
            KIND_TABLET = 1,
        }
        #[repr(u16)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum EngineIteratorSeekType {
            Key = 0,
            First = 1,
            Last = 2,
        }
        #[repr(u8)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum RaftProxyStatus {
            Idle = 0,
            Running = 1,
            Stopped = 2,
        }
        #[repr(u8)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum EngineStoreServerStatus {
            Idle = 0,
            Running = 1,
            Stopping = 2,
            Terminated = 3,
        }
        #[repr(u8)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum RaftstoreVer {
            Uncertain = 0,
            V1 = 1,
            V2 = 2,
        }
        pub type RawCppPtrType = u32;
        pub type RawRustPtrType = u32;
        #[repr(C)]
        #[derive(Debug)]
        pub struct RawRustPtr {
            pub ptr: root::DB::RawVoidPtr,
            pub type_: root::DB::RawRustPtrType,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct RawCppPtr {
            pub ptr: root::DB::RawVoidPtr,
            pub type_: root::DB::RawCppPtrType,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct CppStrWithView {
            pub inner: root::DB::RawCppPtr,
            pub view: root::DB::BaseBuffView,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct PageAndCppStrWithView {
            pub page: root::DB::RawCppPtr,
            pub key: root::DB::RawCppPtr,
            pub page_view: root::DB::BaseBuffView,
            pub key_view: root::DB::BaseBuffView,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct RawCppPtrCarr {
            pub inner: root::DB::RawVoidPtr,
            pub len: u64,
            pub type_: root::DB::RawCppPtrType,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct RawCppPtrTuple {
            pub inner: *mut root::DB::RawCppPtr,
            pub len: u64,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct RawCppPtrArr {
            pub inner: *mut root::DB::RawVoidPtr,
            pub len: u64,
            pub type_: root::DB::RawCppPtrType,
        }
        #[repr(u8)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum HttpRequestStatus {
            Ok = 0,
            ErrorParam = 1,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct HttpRequestRes {
            pub status: root::DB::HttpRequestStatus,
            pub res: root::DB::CppStrWithView,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct CppStrVecView {
            pub view: *const root::DB::BaseBuffView,
            pub len: u64,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct SSTView {
            pub type_: root::DB::ColumnFamilyType,
            pub path: root::DB::BaseBuffView,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct SSTViewVec {
            pub views: *const root::DB::SSTView,
            pub len: u64,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct RaftStoreProxyPtr {
            pub inner: root::DB::ConstRawVoidPtr,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct SSTReaderPtr {
            pub inner: root::DB::RawVoidPtr,
            pub kind: root::DB::SSTFormatKind,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct RustStrWithView {
            pub buff: root::DB::BaseBuffView,
            pub inner: root::DB::RawRustPtr,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct RustStrWithViewVec {
            pub buffs: *const root::DB::BaseBuffView,
            pub len: u64,
            pub inner: root::DB::RawRustPtr,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct SSTReaderInterfaces {
            pub fn_get_sst_reader: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::SSTView,
                    arg2: root::DB::RaftStoreProxyPtr,
                ) -> root::DB::SSTReaderPtr,
            >,
            pub fn_remained: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::SSTReaderPtr,
                    arg2: root::DB::ColumnFamilyType,
                ) -> u8,
            >,
            pub fn_key: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::SSTReaderPtr,
                    arg2: root::DB::ColumnFamilyType,
                ) -> root::DB::BaseBuffView,
            >,
            pub fn_value: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::SSTReaderPtr,
                    arg2: root::DB::ColumnFamilyType,
                ) -> root::DB::BaseBuffView,
            >,
            pub fn_next: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::SSTReaderPtr,
                    arg2: root::DB::ColumnFamilyType,
                ),
            >,
            pub fn_gc: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::SSTReaderPtr,
                    arg2: root::DB::ColumnFamilyType,
                ),
            >,
            pub fn_kind: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::SSTReaderPtr,
                    arg2: root::DB::ColumnFamilyType,
                ) -> root::DB::SSTFormatKind,
            >,
            pub fn_seek: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::SSTReaderPtr,
                    arg2: root::DB::ColumnFamilyType,
                    arg3: root::DB::EngineIteratorSeekType,
                    arg4: root::DB::BaseBuffView,
                ),
            >,
            pub fn_approx_size: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::SSTReaderPtr,
                    arg2: root::DB::ColumnFamilyType,
                ) -> u64,
            >,
            pub fn_get_split_keys: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::SSTReaderPtr,
                    splits_count: u64,
                ) -> root::DB::RustStrWithViewVec,
            >,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct CloudStorageEngineInterfaces {
            pub fn_get_keyspace_encryption: ::std::option::Option<
                unsafe extern "C" fn(arg1: root::DB::RaftStoreProxyPtr, arg2: u32) -> bool,
            >,
            pub fn_get_master_key: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                ) -> root::DB::RawCppStringPtr,
            >,
        }
        #[repr(u32)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum MsgPBType {
            ReadIndexResponse = 0,
            ServerInfoResponse = 1,
            RegionLocalState = 2,
        }
        #[repr(u32)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum KVGetStatus {
            Ok = 0,
            Error = 1,
            NotFound = 2,
        }
        #[repr(u32)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum FastAddPeerStatus {
            Ok = 0,
            WaitForData = 1,
            OtherError = 2,
            NoSuitable = 3,
            BadData = 4,
            FailedInject = 5,
            Canceled = 6,
        }
        #[repr(u8)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum PrehandledSnapshotType {
            None = 0,
            Legacy = 1,
            FAPCheckpoint = 2,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct FastAddPeerRes {
            pub status: root::DB::FastAddPeerStatus,
            pub apply_state: root::DB::CppStrWithView,
            pub region: root::DB::CppStrWithView,
            pub shard_ver: u64,
            pub inner_key: root::DB::CppStrWithView,
            pub enc_key: root::DB::CppStrWithView,
            pub txn_file_ref: root::DB::CppStrWithView,
        }
        #[repr(u32)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum FapSnapshotState {
            NotFound = 0,
            Persisted = 1,
            Other = 2,
        }
        #[repr(u64)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum ConfigJsonType {
            ProxyConfigAddressed = 1,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct RaftStoreProxyFFIHelper {
            pub proxy_ptr: root::DB::RaftStoreProxyPtr,
            pub fn_handle_get_proxy_status: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                ) -> root::DB::RaftProxyStatus,
            >,
            pub fn_is_encryption_enabled: ::std::option::Option<
                unsafe extern "C" fn(arg1: root::DB::RaftStoreProxyPtr) -> u8,
            >,
            pub fn_encryption_method: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                ) -> root::DB::EncryptionMethod,
            >,
            pub fn_handle_get_file: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                    arg2: root::DB::BaseBuffView,
                ) -> root::DB::FileEncryptionInfoRaw,
            >,
            pub fn_handle_new_file: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                    arg2: root::DB::BaseBuffView,
                ) -> root::DB::FileEncryptionInfoRaw,
            >,
            pub fn_handle_delete_file: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                    arg2: root::DB::BaseBuffView,
                ) -> root::DB::FileEncryptionInfoRaw,
            >,
            pub fn_handle_link_file: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                    arg2: root::DB::BaseBuffView,
                    arg3: root::DB::BaseBuffView,
                ) -> root::DB::FileEncryptionInfoRaw,
            >,
            pub fn_handle_batch_read_index: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                    arg2: root::DB::CppStrVecView,
                    arg3: root::DB::RawVoidPtr,
                    arg4: u64,
                    fn_insert_batch_read_index_resp: ::std::option::Option<
                        unsafe extern "C" fn(
                            arg1: root::DB::RawVoidPtr,
                            arg2: root::DB::BaseBuffView,
                            arg3: u64,
                        ),
                    >,
                ),
            >,
            pub sst_reader_interfaces: root::DB::SSTReaderInterfaces,
            pub cloud_storage_engine_interfaces: root::DB::CloudStorageEngineInterfaces,
            pub fn_server_info: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                    arg2: root::DB::BaseBuffView,
                    arg3: root::DB::RawVoidPtr,
                ) -> u32,
            >,
            pub fn_make_read_index_task: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                    arg2: root::DB::BaseBuffView,
                ) -> root::DB::RawRustPtr,
            >,
            pub fn_make_async_waker: ::std::option::Option<
                unsafe extern "C" fn(
                    wake_fn: ::std::option::Option<unsafe extern "C" fn(arg1: root::DB::RawVoidPtr)>,
                    data: root::DB::RawCppPtr,
                ) -> root::DB::RawRustPtr,
            >,
            pub fn_poll_read_index_task: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                    task: root::DB::RawVoidPtr,
                    resp: root::DB::RawVoidPtr,
                    waker: root::DB::RawVoidPtr,
                ) -> u8,
            >,
            pub fn_gc_rust_ptr: ::std::option::Option<
                unsafe extern "C" fn(arg1: root::DB::RawVoidPtr, arg2: root::DB::RawRustPtrType),
            >,
            pub fn_make_timer_task:
                ::std::option::Option<unsafe extern "C" fn(millis: u64) -> root::DB::RawRustPtr>,
            pub fn_poll_timer_task: ::std::option::Option<
                unsafe extern "C" fn(task: root::DB::RawVoidPtr, waker: root::DB::RawVoidPtr) -> u8,
            >,
            pub fn_get_region_local_state: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                    region_id: u64,
                    data: root::DB::RawVoidPtr,
                    error_msg: *mut root::DB::RawCppStringPtr,
                ) -> root::DB::KVGetStatus,
            >,
            pub fn_get_cluster_raftstore_version: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                    refresh_strategy: u8,
                    timeout_ms: i64,
                ) -> root::DB::RaftstoreVer,
            >,
            pub fn_notify_compact_log: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                    region_id: u64,
                    compact_index: u64,
                    compact_term: u64,
                    applied_index: u64,
                ),
            >,
            pub fn_get_config_json: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RaftStoreProxyPtr,
                    kind: root::DB::ConfigJsonType,
                ) -> root::DB::RustStrWithView,
            >,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct PageStorageInterfaces {
            pub fn_create_write_batch: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *const root::DB::EngineStoreServerWrap,
                ) -> root::DB::RawCppPtr,
            >,
            pub fn_wb_put_page: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RawVoidPtr,
                    arg2: root::DB::BaseBuffView,
                    arg3: root::DB::BaseBuffView,
                ),
            >,
            pub fn_wb_del_page: ::std::option::Option<
                unsafe extern "C" fn(arg1: root::DB::RawVoidPtr, arg2: root::DB::BaseBuffView),
            >,
            pub fn_get_wb_size:
                ::std::option::Option<unsafe extern "C" fn(arg1: root::DB::RawVoidPtr) -> u64>,
            pub fn_is_wb_empty:
                ::std::option::Option<unsafe extern "C" fn(arg1: root::DB::RawVoidPtr) -> u8>,
            pub fn_handle_merge_wb: ::std::option::Option<
                unsafe extern "C" fn(arg1: root::DB::RawVoidPtr, arg2: root::DB::RawVoidPtr),
            >,
            pub fn_handle_clear_wb:
                ::std::option::Option<unsafe extern "C" fn(arg1: root::DB::RawVoidPtr)>,
            pub fn_handle_consume_wb: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *const root::DB::EngineStoreServerWrap,
                    arg2: root::DB::RawVoidPtr,
                ),
            >,
            pub fn_handle_read_page: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *const root::DB::EngineStoreServerWrap,
                    arg2: root::DB::BaseBuffView,
                ) -> root::DB::CppStrWithView,
            >,
            pub fn_handle_scan_page: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *const root::DB::EngineStoreServerWrap,
                    arg2: root::DB::BaseBuffView,
                    arg3: root::DB::BaseBuffView,
                ) -> root::DB::RawCppPtrCarr,
            >,
            pub fn_handle_get_lower_bound: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *const root::DB::EngineStoreServerWrap,
                    arg2: root::DB::BaseBuffView,
                ) -> root::DB::CppStrWithView,
            >,
            pub fn_is_ps_empty: ::std::option::Option<
                unsafe extern "C" fn(arg1: *const root::DB::EngineStoreServerWrap) -> u8,
            >,
            pub fn_handle_purge_ps: ::std::option::Option<
                unsafe extern "C" fn(arg1: *const root::DB::EngineStoreServerWrap),
            >,
        }
        #[repr(u64)]
        #[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
        pub enum ReportThreadAllocateInfoType {
            Reset = 0,
            Remove = 1,
            AllocPtr = 2,
            DeallocPtr = 3,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct ReportThreadAllocateInfoBatch {
            pub alloc: u64,
            pub dealloc: u64,
        }
        #[repr(C)]
        #[derive(Debug)]
        pub struct EngineStoreServerHelper {
            pub magic_number: u32,
            pub version: u64,
            pub inner: *mut root::DB::EngineStoreServerWrap,
            pub ps: root::DB::PageStorageInterfaces,
            pub fn_gen_cpp_string: ::std::option::Option<
                unsafe extern "C" fn(arg1: root::DB::BaseBuffView) -> root::DB::RawCppPtr,
            >,
            pub fn_handle_write_raft_cmd: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *const root::DB::EngineStoreServerWrap,
                    arg2: root::DB::WriteCmdsView,
                    arg3: root::DB::RaftCmdHeader,
                ) -> root::DB::EngineStoreApplyRes,
            >,
            pub fn_handle_admin_raft_cmd: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *const root::DB::EngineStoreServerWrap,
                    arg2: root::DB::BaseBuffView,
                    arg3: root::DB::BaseBuffView,
                    arg4: root::DB::RaftCmdHeader,
                ) -> root::DB::EngineStoreApplyRes,
            >,
            pub fn_need_flush_data: ::std::option::Option<
                unsafe extern "C" fn(arg1: *mut root::DB::EngineStoreServerWrap, arg2: u64) -> u8,
            >,
            pub fn_try_flush_data: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    region_id: u64,
                    flush_pattern: u8,
                    index: u64,
                    term: u64,
                    truncated_index: u64,
                    truncated_term: u64,
                ) -> u8,
            >,
            pub fn_atomic_update_proxy: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    arg2: *mut root::DB::RaftStoreProxyFFIHelper,
                ),
            >,
            pub fn_handle_destroy: ::std::option::Option<
                unsafe extern "C" fn(arg1: *mut root::DB::EngineStoreServerWrap, arg2: u64),
            >,
            pub fn_handle_ingest_sst: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    arg2: root::DB::SSTViewVec,
                    arg3: root::DB::RaftCmdHeader,
                ) -> root::DB::EngineStoreApplyRes,
            >,
            pub fn_handle_compute_store_stats: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                ) -> root::DB::StoreStats,
            >,
            pub fn_handle_get_engine_store_server_status: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                ) -> root::DB::EngineStoreServerStatus,
            >,
            pub fn_pre_handle_snapshot: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    arg2: root::DB::BaseBuffView,
                    arg3: u64,
                    arg4: root::DB::SSTViewVec,
                    arg5: u64,
                    arg6: u64,
                ) -> root::DB::RawCppPtr,
            >,
            pub fn_apply_pre_handled_snapshot: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    arg2: root::DB::RawVoidPtr,
                    arg3: root::DB::RawCppPtrType,
                ),
            >,
            pub fn_abort_pre_handle_snapshot: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    arg2: u64,
                    arg3: u64,
                ),
            >,
            pub fn_release_pre_handled_snapshot: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    arg2: root::DB::RawVoidPtr,
                    arg3: root::DB::RawCppPtrType,
                ),
            >,
            pub fn_apply_fap_snapshot: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    arg2: u64,
                    arg3: u64,
                    arg4: u8,
                    arg5: u64,
                    arg6: u64,
                ) -> u8,
            >,
            pub fn_handle_http_request: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    path: root::DB::BaseBuffView,
                    query: root::DB::BaseBuffView,
                    body: root::DB::BaseBuffView,
                ) -> root::DB::HttpRequestRes,
            >,
            pub fn_check_http_uri_available:
                ::std::option::Option<unsafe extern "C" fn(arg1: root::DB::BaseBuffView) -> u8>,
            pub fn_gc_raw_cpp_ptr: ::std::option::Option<
                unsafe extern "C" fn(arg1: root::DB::RawVoidPtr, arg2: root::DB::RawCppPtrType),
            >,
            pub fn_gc_raw_cpp_ptr_carr: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RawVoidPtr,
                    arg2: root::DB::RawCppPtrType,
                    arg3: u64,
                ),
            >,
            pub fn_gc_special_raw_cpp_ptr: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: root::DB::RawVoidPtr,
                    arg2: u64,
                    arg3: root::DB::SpecialCppPtrType,
                ),
            >,
            pub fn_get_config: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    full: u8,
                ) -> root::DB::CppStrWithView,
            >,
            pub fn_set_store: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    arg2: root::DB::BaseBuffView,
                ),
            >,
            pub fn_set_pb_msg_by_bytes: ::std::option::Option<
                unsafe extern "C" fn(
                    type_: root::DB::MsgPBType,
                    ptr: root::DB::RawVoidPtr,
                    buff: root::DB::BaseBuffView,
                ),
            >,
            pub fn_handle_safe_ts_update: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    region_id: u64,
                    self_safe_ts: u64,
                    leader_safe_ts: u64,
                ),
            >,
            pub fn_fast_add_peer: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    region_id: u64,
                    new_peer_id: u64,
                ) -> root::DB::FastAddPeerRes,
            >,
            pub fn_get_lock_by_key: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *const root::DB::EngineStoreServerWrap,
                    arg2: u64,
                    arg3: root::DB::BaseBuffView,
                ) -> root::DB::BaseBuffView,
            >,
            pub fn_query_fap_snapshot_state: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    region_id: u64,
                    new_peer_id: u64,
                    arg2: u64,
                    arg3: u64,
                ) -> root::DB::FapSnapshotState,
            >,
            pub fn_clear_fap_snapshot: ::std::option::Option<
                unsafe extern "C" fn(arg1: *mut root::DB::EngineStoreServerWrap, region_id: u64),
            >,
            pub fn_kvstore_region_exists: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    region_id: u64,
                ) -> bool,
            >,
            pub fn_report_thread_allocate_info: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    thread_id: u64,
                    name: root::DB::BaseBuffView,
                    type_: root::DB::ReportThreadAllocateInfoType,
                    value: u64,
                ),
            >,
            pub fn_report_thread_allocate_batch: ::std::option::Option<
                unsafe extern "C" fn(
                    arg1: *mut root::DB::EngineStoreServerWrap,
                    thread_id: u64,
                    name: root::DB::BaseBuffView,
                    data: root::DB::ReportThreadAllocateInfoBatch,
                ),
            >,
        }
        extern "C" {
            pub fn ffi_get_server_info_from_proxy(
                arg1: isize,
                arg2: root::DB::BaseBuffView,
                arg3: root::DB::RawVoidPtr,
            ) -> u32;
        }
        pub const RAFT_STORE_PROXY_VERSION: u64 = 2149052863435660119;
        pub const RAFT_STORE_PROXY_MAGIC_NUMBER: u32 = 324508639;
    }
}

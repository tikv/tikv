#pragma once

#include "ColumnFamily.h"

namespace DB {

struct EngineStoreServerWrap;

enum class EngineStoreApplyRes : uint32_t {
  None = 0,
  Persist,
  NotFound,
};

enum class WriteCmdType : uint8_t {
  Put = 0,
  Del,
};

struct BaseBuffView {
  const char *data;
  const uint64_t len;

  BaseBuffView(const char *data_ = nullptr, const uint64_t len_ = 0)
      : data(data_), len(len_) {}
};

struct RaftCmdHeader {
  uint64_t region_id;
  uint64_t index;
  uint64_t term;
};

struct WriteCmdsView {
  const BaseBuffView *keys;
  const BaseBuffView *vals;
  const WriteCmdType *cmd_types;
  const ColumnFamilyType *cmd_cf;
  const uint64_t len;
};

struct FsStats {
  uint64_t used_size;
  uint64_t avail_size;
  uint64_t capacity_size;

  uint8_t ok;

  FsStats() : used_size(0), avail_size(0), capacity_size(0), ok(0) {}
};

struct StoreStats {
  FsStats fs_stats;

  uint64_t engine_bytes_written;
  uint64_t engine_keys_written;
  uint64_t engine_bytes_read;
  uint64_t engine_keys_read;

  StoreStats()
      : fs_stats(),
        engine_bytes_written(0),
        engine_keys_written(0),
        engine_bytes_read(0),
        engine_keys_read(0) {}
};

enum class RaftProxyStatus : uint8_t {
  Idle = 0,
  Running,
  Stopped,
};

enum class EngineStoreServerStatus : uint8_t {
  Idle = 0,
  Running,
  Stopped,
};

enum class RawCppPtrType : uint32_t {
  None = 0,
  String,
  PreHandledSnapshot,
};

struct RawCppPtr {
  RawVoidPtr ptr;
  RawCppPtrType type;

  RawCppPtr(RawVoidPtr ptr_ = nullptr,
            RawCppPtrType type_ = RawCppPtrType::None)
      : ptr(ptr_), type(type_) {}
  RawCppPtr(const RawCppPtr &) = delete;
  RawCppPtr(RawCppPtr &&) = delete;
};

struct CppStrWithView {
  RawCppPtr inner;
  BaseBuffView view;

  CppStrWithView(const CppStrWithView &) = delete;
};

enum class HttpRequestStatus : uint8_t {
  Ok = 0,
  ErrorParam,
};

struct HttpRequestRes {
  HttpRequestStatus status;
  CppStrWithView res;

  HttpRequestRes(const HttpRequestRes &) = delete;
};

struct CppStrVecView {
  const BaseBuffView *view;
  uint64_t len;
};

struct FileEncryptionInfoRaw;
enum class EncryptionMethod : uint8_t;

struct SSTView {
  ColumnFamilyType type;
  BaseBuffView path;
};

struct SSTViewVec {
  const SSTView *views;
  const uint64_t len;
};

struct RaftStoreProxyPtr {
  ConstRawVoidPtr inner;
};

struct SSTReaderPtr {
  RawVoidPtr inner;
};

struct SSTReaderInterfaces {
  SSTReaderPtr (*fn_get_sst_reader)(SSTView, RaftStoreProxyPtr);
  uint8_t (*fn_remained)(SSTReaderPtr, ColumnFamilyType);
  BaseBuffView (*fn_key)(SSTReaderPtr, ColumnFamilyType);
  BaseBuffView (*fn_value)(SSTReaderPtr, ColumnFamilyType);
  void (*fn_next)(SSTReaderPtr, ColumnFamilyType);
  void (*fn_gc)(SSTReaderPtr, ColumnFamilyType);
};

struct RaftStoreProxyFFIHelper {
  RaftStoreProxyPtr proxy_ptr;
  RaftProxyStatus (*fn_handle_get_proxy_status)(RaftStoreProxyPtr);
  uint8_t (*fn_is_encryption_enabled)(RaftStoreProxyPtr);
  EncryptionMethod (*fn_encryption_method)(RaftStoreProxyPtr);
  FileEncryptionInfoRaw (*fn_handle_get_file)(RaftStoreProxyPtr, BaseBuffView);
  FileEncryptionInfoRaw (*fn_handle_new_file)(RaftStoreProxyPtr, BaseBuffView);
  FileEncryptionInfoRaw (*fn_handle_delete_file)(RaftStoreProxyPtr,
                                                 BaseBuffView);
  FileEncryptionInfoRaw (*fn_handle_link_file)(RaftStoreProxyPtr, BaseBuffView,
                                               BaseBuffView);
  RawVoidPtr (*fn_handle_batch_read_index)(RaftStoreProxyPtr, CppStrVecView);
  SSTReaderInterfaces sst_reader_interfaces;
};

struct EngineStoreServerHelper {
  uint32_t magic_number;  // use a very special number to check whether this
                          // struct is legal
  uint32_t version;       // version of function interface
  //

  EngineStoreServerWrap *inner;
  RawCppPtr (*fn_gen_cpp_string)(BaseBuffView);
  EngineStoreApplyRes (*fn_handle_write_raft_cmd)(const EngineStoreServerWrap *,
                                                  WriteCmdsView, RaftCmdHeader);
  EngineStoreApplyRes (*fn_handle_admin_raft_cmd)(const EngineStoreServerWrap *,
                                                  BaseBuffView, BaseBuffView,
                                                  RaftCmdHeader);
  void (*fn_atomic_update_proxy)(EngineStoreServerWrap *,
                                 RaftStoreProxyFFIHelper *);
  void (*fn_handle_destroy)(EngineStoreServerWrap *, uint64_t);
  EngineStoreApplyRes (*fn_handle_ingest_sst)(EngineStoreServerWrap *,
                                              SSTViewVec, RaftCmdHeader);
  uint8_t (*fn_handle_check_terminated)(EngineStoreServerWrap *);
  StoreStats (*fn_handle_compute_store_stats)(EngineStoreServerWrap *);
  EngineStoreServerStatus (*fn_handle_get_engine_store_server_status)(
      EngineStoreServerWrap *);
  RawCppPtr (*fn_pre_handle_snapshot)(EngineStoreServerWrap *, BaseBuffView,
                                      uint64_t, SSTViewVec, uint64_t, uint64_t);
  void (*fn_apply_pre_handled_snapshot)(EngineStoreServerWrap *, RawVoidPtr,
                                        RawCppPtrType);
  HttpRequestRes (*fn_handle_http_request)(EngineStoreServerWrap *,
                                           BaseBuffView);
  uint8_t (*fn_check_http_uri_available)(BaseBuffView);
  void (*fn_gc_raw_cpp_ptr)(EngineStoreServerWrap *, RawVoidPtr, RawCppPtrType);
  RawVoidPtr (*fn_gen_batch_read_index_res)(uint64_t);
  void (*fn_insert_batch_read_index_resp)(RawVoidPtr, BaseBuffView, uint64_t);
};
}  // namespace DB

#pragma once

#include "Common.h"

namespace DB {
enum class FileEncryptionRes : uint8_t {
  Disabled = 0,
  Ok,
  Error,
};

enum class EncryptionMethod : uint8_t {
  Unknown = 0,
  Plaintext,
  Aes128Ctr,
  Aes192Ctr,
  Aes256Ctr,
  SM4Ctr,
};
struct FileEncryptionInfoRaw {
  FileEncryptionRes res;
  EncryptionMethod method;
  RawCppStringPtr key;
  RawCppStringPtr iv;
  RawCppStringPtr error_msg;
};
}  // namespace DB

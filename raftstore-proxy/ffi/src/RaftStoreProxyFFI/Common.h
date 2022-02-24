#pragma once

#include <cstdint>

namespace DB {
using ConstRawVoidPtr = const void *;
using RawVoidPtr = void *;

struct RawCppString;
using RawCppStringPtr = RawCppString *;

}  // namespace DB

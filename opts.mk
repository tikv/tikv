# Figure out the correct set of rustc/gcc/msvc compiler optimization flags for
# the desired platform. This mostly sets the `RUSTFLAGS`, `CXXFLAGS`, and
# `CFLAGS` environment variables, but it also sets the features enabled in the
# RocksDB crate.
#
# On x86_64 we explicitly set the minimum architecture as "Westmere", a CPU from
# 2010. We do this because we want to use RocksDB's SSE4.2-optimized Fast_CRC32
# function. That optimization also uses the PCLMUL instruction, and that
# instruction was introduced in Westmere (where SSE4.2 was introduced in
# Nehalem), so we might as well use Westmere-level optimizations everywhere.
#
# Further, we don't want post-Westmere instructions because of tikv#4999 where
# a user's KVM environment did not support AVX.
#
# The behavior of this file can be controlled with the `OPTIMIZE` environment
# variable, the possible values of which are:
#
# - "normal" - optimize for the production target platform, Westmere for x86_64
#
# - "safe" - optimize for all CPUS in the target family, for x86_64 this is a
#   2000-era Core 1 CPU.
#
# - "native" - optimize for the local CPU
#
# - "none" - don't control the optimizations. You probably don't want this -
#   most code will be compiled like "safe", but RocksDB will be compiled like
#   "native".
#
# This file _prepends_ `RUSTFLAGS` with `-Ctarget-cpu`, and prepends both both
# `CXXFLAGS` and `CFLAGS` with both `-march` and `-mtune`, so the caller can
# still control these flags by setting them in the environment variable
# themselves - the compiler prefers the last e.g. `-march` on the command line.
#
# The `TARGET` environment variable can control the target being optimized for,
# though cross-compilation support is limited. If `TARGET` is not set, the
# native CPU will be used to determine optimization settings.

# These are the variables we are basing our flags off of
GCCISH=0
AMD64ISH=0

# Either interpret `TARGET` as a target triple (for e.g. cross-compile), or
# figure out the native platform from `uname`
ifndef TARGET

KERNEL=$(shell uname -s)
MACHINE=$(shell uname -m)

ifeq ($(KERNEL),Linux)
GCCISH=1
else ifeq ($(KERNEL),Darwin)
GCCISH=1
endif

ifeq ($(MACHINE),x86_64)
AMD64ISH=1
endif

else # TARGET is defined

ifneq (,$(findstring x86_64,$(TARGET)))
AMD64ISH=1
endif

ifneq (,$(findstring linux,$(TARGET)))
GCCISH=1
endif

ifneq (,$(findstring darwin,$(TARGET)))
GCCISH=1
endif

endif # TARGET

# Possible values: "normal", "safe", "native", "none"
OPTIMIZE?=normal

# Note that the RocksDB CMakefile and Rust crate build with -march=native by
# default and contain custom config options for "portable" builds. We're going
# to configure the rocks optimizations ourselves by setting -march, etc directly
# through CXXFLAGS. To do that though we have to turn on the "portable" build,
# which causes the rocks build to not pass any -march flags (the gcc default is
# -march=x86_64).

# FIXME: gcc distinguishes between the instruction set used for codegen and the
# machine architecture codegen is tuned for. So you can use instructions for
# -march=westmere, but _tune_ the CPU for -mtune=generic (-mcpu is like -march +
# -mtune). We are explicitly setting -march and -mtune for gcc.
#
# rustc only has -Ctarget-cpu, which is used to annotate all functions with an
# LLVM "target-cpu" attribute, and also passed to the gcc linker plugin as
# -mcpu, so here we are telling gcc/clang to use e.g. westmere instructions
# while using a "generic" x86_64 tuning model, but _seemingly_ telling LLVM to
# use westmere instructions with a westmere tuning model.
#
# The impact of this in not clear - it may be that LLVM doesn't distinguish
# between -march and -mtune like gcc does. More investigation needed.

# For now, only pass optimization flags on platforms with gcc-ish C compilers.
ifeq ($(GCCISH),1)
ifeq ($(AMD64ISH),1)

ifeq ($(OPTIMIZE),normal)
# Optimize for Westmere, the first x86_64 arch with SSE 4.2. RocksDB has a
# crucial SSE 4.2 optimization that we strongly want to turn on, so Westmere is
# our baseline CPU.

# FIXME: For gcc we're actually _not_ using -march=westmere, but -march=corei7
# because the TiKV build machines are running a gcc 4.8.5 that doesn't know the
# -march=westmere flag. -march=corei7 appears to be equivalent to
# -march=nehalem, one generation back from westmere. In effect this means that
# gcc will not have access to the AES instructions, including PCLMUL. The
# RocksDB usage of PCLMUL is open-coded though, so the Fast_CRC32 optimization
# still works.
RUSTFLAGS:=-Ctarget-cpu=westmere $(RUSTFLAGS)
CXXFLAGS:=-march=corei7 $(CXXFLAGS)
CXXFLAGS:=-mtune=generic $(CXXFLAGS)
CFLAGS:=-march=corei7 $(CFLAGS)
CFLAGS:=-mtune=generic $(CFLAGS)
# Turning _on_ the portable RocksDB build makes it not pass any -march flags,
# letting us override -march. Turning on SSE explicitly is required for the key
# hand-rolled Fast_CRC32 optimization.
ROCKSDB_SYS_PORTABLE?=1
ROCKSDB_SYS_SSE?=1

else ifeq ($(OPTIMIZE),safe)
# Optimize explicitly for baseline x86_64. For most codebases and compilers this
# is the default, but notably not for the RocksDB build system, for which we
# must turn on "portable" mode.

RUSTFLAGS:=-Ctarget-cpu=x86-64 $(RUSTFLAGS )
CXXFLAGS:=-march=x86-64 $(CXXFLAGS)
CXXFLAGS:=-mtune=generic $(CXXFLAGS)
CFLAGS:=-march=x86-64 $(CFLAGS)
CFLAGS:=-mtune=generic $(CFLAGS)
ROCKSDB_SYS_PORTABLE?=1
ROCKSDB_SYS_SSE?=0

else ifeq ($(OPTIMIZE),native)
# Optimize for the local CPU. This is not recommended. If somebody truly wants
# to customize their optimization settings they should override
# RUSTFLAGS/CXXFLAGS/CFLAGS.

RUSTFLAGS:=-Ctarget-cpu=native $(RUSTFLAGS)
CXXFLAGS:=-march=native $(CXXFLAGS)
CXXFLAGS:=-mtune=native $(CXXFLAGS)
CFLAGS:=-march=native $(CFLAGS)
CFLAGS:=-mtune=native $(CFLAGS)
# Assume that the native CPU supports SSE4.2 to enable the explicit RocksDB CRC
# optimization
ROCKSDB_SYS_PORTABLE?=0
ROCKSDB_SYS_SSE?=1

endif

export RUSTFLAGS
export CXXFLAGS
export CFLAGS

else # !ifeq AMD64ISH

# Some basic settings for non-AMD64 architectures
ROCKSDB_SYS_PORTABLE?=1
ROCKSDB_SYS_SSE?=1 # Could make sense on 32-bit x686

endif # ifeq AMD64ISH

else # !ifeq GCCISH

# Some basic settings for MSVC toolchains
ROCKSDB_SYS_PORTABLE?=1
ROCKSDB_SYS_SSE?=1

endif # ifeq GCCISH

# Just dump the results of all the above
target-opt-test:
	$(info $$TARGET is [$(TARGET)])
	$(info $$AMD64ISH is [$(AMD64ISH)])
	$(info $$GCCISH is [$(GCCISH)])
	$(info $$RUSTFLAGS is [$(RUSTFLAGS)])
	$(info $$CXXFLAGS is [$(CXXFLAGS)])
	$(info $$CFLAGS is [$(CFLAGS)])
	$(info $$ROCKSDB_SYS_SSE is [$(ROCKSDB_SYS_SSE)])
	$(info $$ROCKSDB_SYS_PORTABLE is [$(ROCKSDB_SYS_PORTABLE)])


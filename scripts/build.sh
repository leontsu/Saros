#!/bin/bash
# Usage: scripts/build.sh [build_dir]  (default: build; JOBS=n, default nproc)
# Builds Masstree and mimalloc into <build_dir>/deps, then the binaries into <build_dir>/bin (Release).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BUILD=$(realpath -m "${1:-$ROOT/build}")
JOBS=${JOBS:-$(nproc)}
DEPS=$BUILD/deps
mkdir -p "$DEPS"

if [ ! -f "$DEPS/masstree/libkohler_masstree_json.a" ]; then
  rm -rf "$DEPS/masstree"
  mkdir -p "$DEPS/masstree"
  cp -R "$ROOT/third_party/masstree/." "$DEPS/masstree"
  (cd "$DEPS/masstree" &&
   ./bootstrap.sh &&
   ./configure --disable-assertions &&
   make -j"$JOBS" CXXFLAGS='-g -W -Wall -O3 -fPIC' &&
   ar cr libkohler_masstree_json.a json.o string.o straccum.o str.o msgpack.o \
     clp.o kvrandom.o compiler.o memdebug.o kvthread.o misc.o &&
   ranlib libkohler_masstree_json.a)
fi

if [ ! -f "$DEPS/mimalloc/libmimalloc.a" ]; then
  cmake -S "$ROOT/third_party/mimalloc" -B "$DEPS/mimalloc" -DCMAKE_BUILD_TYPE=Release
  cmake --build "$DEPS/mimalloc" -j"$JOBS"
fi

cmake -S "$ROOT" -B "$BUILD" -DCMAKE_BUILD_TYPE=Release -DDEPS_DIR="$DEPS"
cmake --build "$BUILD" -j"$JOBS"

# TuskFlow's C++ port (submodule TuskFlow/, Fig. 7) into <build_dir>/TuskFlow. It needs CMake 3.14+ and
# fetches mimalloc v2.3.2 at configure time.
if [ -f "$ROOT/TuskFlow/CMakeLists.txt" ]; then
  cmake -S "$ROOT/TuskFlow" -B "$BUILD/TuskFlow" -DCMAKE_BUILD_TYPE=Release
  cmake --build "$BUILD/TuskFlow" -j"$JOBS"
fi

#!/bin/bash

set -euo pipefail

BUILD_DIR="build"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
JOBS="${JOBS:-$(nproc)}"

need_configure=false

if [[ ! -d "$BUILD_DIR" ]]; then
    mkdir -p "$BUILD_DIR"
    need_configure=true
fi

if [[ ! -f "$BUILD_DIR/CMakeCache.txt" ]]; then
    need_configure=true
fi

# If any CMake config file is newer than cache, refresh CMake configure.
if [[ "$need_configure" == false ]]; then
    if [[ "CMakeLists.txt" -nt "$BUILD_DIR/CMakeCache.txt" ]]; then
        need_configure=true
    else
        while IFS= read -r cmake_file; do
            if [[ "$cmake_file" -nt "$BUILD_DIR/CMakeCache.txt" ]]; then
                need_configure=true
                break
            fi
        done < <(find . -name "*.cmake" -type f)
    fi
fi

if [[ "$need_configure" == true ]]; then
    cmake -S . -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
fi

# Incremental build: only changed files/targets will be rebuilt.
cmake --build "$BUILD_DIR" -- -j"$JOBS"
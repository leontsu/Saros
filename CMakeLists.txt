cmake_minimum_required(VERSION 3.10)

project(saros LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

set(THIRD_PARTY_DIR "${PROJECT_SOURCE_DIR}/third_party")
# scripts/build.sh builds Masstree and mimalloc into ${DEPS_DIR}.
set(DEPS_DIR "${CMAKE_BINARY_DIR}/deps" CACHE PATH "Built Masstree and mimalloc")
set(MASSTREE_LIB "${DEPS_DIR}/masstree/libkohler_masstree_json.a")
set(MIMALLOC_LIB "${DEPS_DIR}/mimalloc/libmimalloc.a")
include_directories(${DEPS_DIR})

option(ENABLE_SANITIZER "enable address sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" OFF)
option(ENABLE_TSAN "enable thread sanitizer on debug build (instead of address sanitizer)" OFF)
option(BUILD_TESTING "build the tests" ON)

find_package(Threads REQUIRED)
find_package(gflags REQUIRED)
find_package(glog REQUIRED)

include(CompileOptions)

add_subdirectory(saros)
if (BUILD_TESTING)
    enable_testing()
    add_subdirectory(tests)
endif ()

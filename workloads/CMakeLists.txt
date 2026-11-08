# Each driver is compiled once per engine; ENGINE_SAROS selects the Saros-only parts.
function(add_benchmark name source engine)
    add_executable(${name} ${source}
            "${PROJECT_SOURCE_DIR}/common/result.cc"
            "${PROJECT_SOURCE_DIR}/common/util.cc"
            "${PROJECT_SOURCE_DIR}/common/engine/result.cc"
            "${PROJECT_SOURCE_DIR}/${engine}/transaction.cc"
            "${PROJECT_SOURCE_DIR}/${engine}/util.cc")
    target_include_directories(${name} PRIVATE "${PROJECT_SOURCE_DIR}/${engine}")
    if (engine STREQUAL "saros")
        target_compile_definitions(${name} PRIVATE ENGINE_SAROS)
    endif ()
    target_link_libraries(${name} gflags::gflags glog::glog ${MIMALLOC_LIB} ${MASSTREE_LIB} Threads::Threads)
endfunction()

add_benchmark(bomb_saros bomb.cc saros)
add_benchmark(ldbc_saros ldbc.cc saros)
add_benchmark(bomb_oze bomb.cc oze)
add_benchmark(ldbc_oze ldbc.cc oze)

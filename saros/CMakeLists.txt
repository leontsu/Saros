set(SAROS_SOURCES
        "${PROJECT_SOURCE_DIR}/common/result.cc"
        "${PROJECT_SOURCE_DIR}/common/util.cc"
        result.cc
        transaction.cc
        util.cc
        )

set(SAROS_LIBRARIES
        gflags::gflags
        glog::glog
        ${MIMALLOC_LIB}
        ${MASSTREE_LIB}
        Threads::Threads
        )

add_executable(bomb_saros bomb.cc ${SAROS_SOURCES})
target_link_libraries(bomb_saros ${SAROS_LIBRARIES})

add_executable(ldbc_saros ldbc.cc ${SAROS_SOURCES})
target_link_libraries(ldbc_saros ${SAROS_LIBRARIES})

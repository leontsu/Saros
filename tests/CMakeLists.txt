set(TEST_SUPPORT_SOURCES
        "${PROJECT_SOURCE_DIR}/common/result.cc"
        "${PROJECT_SOURCE_DIR}/common/util.cc"
        "${PROJECT_SOURCE_DIR}/saros/result.cc"
        "${PROJECT_SOURCE_DIR}/saros/transaction.cc"
        "${PROJECT_SOURCE_DIR}/saros/util.cc"
        "${THIRD_PARTY_DIR}/googletest/googletest/src/gtest-all.cc"
        "${THIRD_PARTY_DIR}/googletest/googletest/src/gtest_main.cc"
        )

function(add_saros_test name)
    add_executable(${name} ${name}.cpp ${TEST_SUPPORT_SOURCES})
    target_include_directories(${name}
            PRIVATE ${PROJECT_SOURCE_DIR}
            PRIVATE ${PROJECT_SOURCE_DIR}/saros
            PRIVATE ${THIRD_PARTY_DIR}/googletest/googletest
            PRIVATE ${THIRD_PARTY_DIR}/googletest/googletest/include
            )
    target_link_libraries(${name}
            gflags::gflags
            glog::glog
            ${MIMALLOC_LIB}
            ${MASSTREE_LIB}
            Threads::Threads
            )
    add_test(NAME ${name} COMMAND ${name})
endfunction()

add_saros_test(history_test)
add_saros_test(retirement_test)
add_saros_test(schedule_test)
add_saros_test(chunk_freeing_test)

set(TEST_LIBRARIES gflags::gflags glog::glog ${MIMALLOC_LIB} ${MASSTREE_LIB} Threads::Threads)

# A test binary of one engine; `source` is its source file without extension.
function(add_engine_test name source engine)
    add_executable(${name} ${source}.cpp
            "${PROJECT_SOURCE_DIR}/common/result.cc"
            "${PROJECT_SOURCE_DIR}/common/util.cc"
            "${PROJECT_SOURCE_DIR}/${engine}/result.cc"
            "${PROJECT_SOURCE_DIR}/${engine}/transaction.cc"
            "${PROJECT_SOURCE_DIR}/${engine}/util.cc"
            "${THIRD_PARTY_DIR}/googletest/googletest/src/gtest-all.cc"
            "${THIRD_PARTY_DIR}/googletest/googletest/src/gtest_main.cc")
    target_include_directories(${name}
            PRIVATE ${PROJECT_SOURCE_DIR}/${engine}
            PRIVATE ${THIRD_PARTY_DIR}/googletest/googletest
            PRIVATE ${THIRD_PARTY_DIR}/googletest/googletest/include)
    if (engine STREQUAL "saros")
        target_compile_definitions(${name} PRIVATE ENGINE_SAROS)
    endif ()
    target_link_libraries(${name} ${TEST_LIBRARIES})
    set_target_properties(${name} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests")
    add_test(NAME ${name} COMMAND ${name})
endfunction()

add_engine_test(history_test history_test saros)
add_engine_test(retirement_test retirement_test saros)
add_engine_test(schedule_test schedule_test saros)
add_engine_test(chunk_freeing_test chunk_freeing_test saros)
add_engine_test(insert_only_rule_test_saros insert_only_rule_test saros)

set(BUILD_TESTING ON)
include(CTest)

add_library(velox4j_test INTERFACE)
target_include_directories(velox4j_test INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(
  velox4j_test
  INTERFACE
    velox4j_shared
    velox_vector_test_lib
    velox_dwio_common_test_utils
    velox_file_test_utils
    velox_test_util
    velox_exec_test_lib)

# velox_rpc_node_test (from Velox) includes <gmock/gmock.h> but its upstream
# CMakeLists.txt omits GTest::gmock from its link libraries, so the gmock
# headers are never added to the include path. Add the missing dependency here.
if(TARGET velox_rpc_node_test)
  target_link_libraries(velox_rpc_node_test GTest::gmock)
endif()
set(TEST_OBJECT_FILES $<TARGET_OBJECTS:velox_fuzzer_connector>)

add_executable(velox4j_query_serde_test ${TEST_OBJECT_FILES}
                                        velox4j/query/QuerySerdeTest.cc)
target_link_libraries(
  velox4j_query_serde_test velox4j_test)
add_test(velox4j_query_serde_test velox4j_query_serde_test)

add_executable(velox4j_query_test ${TEST_OBJECT_FILES}
                                  velox4j/query/QueryTest.cc)
target_link_libraries(
  velox4j_query_test velox4j_test)
add_test(velox4j_query_test velox4j_query_test)

add_executable(velox4j_blocking_queue_test
               ${TEST_OBJECT_FILES} velox4j/iterator/BlockingQueueTest.cc)
target_link_libraries(
  velox4j_blocking_queue_test velox4j_test)
add_test(velox4j_blocking_queue_test velox4j_blocking_queue_test)

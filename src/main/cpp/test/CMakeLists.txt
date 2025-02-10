add_library(velox4j-test INTERFACE)
target_include_directories(velox4j-test INTERFACE
        ${CMAKE_CURRENT_LIST_DIR}
)
target_link_libraries(velox4j-test INTERFACE velox4j-shared velox_vector_test_lib velox_dwio_common_test_utils velox_file_test_utils velox_temp_path velox_exec_test_lib)

add_executable(velox4j-query-serde-test velox4j/query/QuerySerdeTest.cc)
target_link_libraries(velox4j-query-serde-test velox4j-test)

function(import_library TARGET_NAME LIB_PATH)
    add_library(${TARGET_NAME} STATIC IMPORTED)
    set_target_properties(${TARGET_NAME} PROPERTIES IMPORTED_LOCATION ${LIB_PATH})
endfunction()

import_library(velox::vector_test_lib ${velox_BUILD_DIR}/velox/vector/tests/utils/libvelox_vector_test_lib.a)
import_library(velox::dwio_common_test ${velox_BUILD_DIR}/velox/dwio/common/tests/utils/libvelox_dwio_common_test_utils.a)
import_library(velox::file_test_utils ${velox_BUILD_DIR}/velox/common/file/tests/libvelox_file_test_utils.a)
import_library(velox::temp_path ${velox_BUILD_DIR}/velox/exec/tests/utils/libvelox_temp_path.a)
import_library(velox::exec_test_lib ${velox_BUILD_DIR}/velox/exec/tests/utils/libvelox_exec_test_lib.a)

add_library(velox4j-test INTERFACE)
target_include_directories(velox4j-test INTERFACE ${VELOX4J_SOURCE_DIR})
target_link_libraries(velox4j-test INTERFACE velox4j velox::vector_test_lib velox::dwio_common_test velox::file_test_utils velox::temp_path velox::exec_test_lib)

add_executable(velox4j-dummy-test ${CMAKE_CURRENT_LIST_DIR}/DummyTest.cc)
target_link_libraries(velox4j-dummy-test velox4j-test)

cmake_minimum_required(VERSION 3.29)
project(velox4jcpp)

include(cmake/CPM.cmake)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

set(VELOX_REF 13e72be62464bf67da925dad764c89753e159299)
set(VELOX_URL_MD5 ebbfec532e1ad3fe6598279da70fe56e)
set(VELOX_SOURCE_URL "https://github.com/zhztheplayer/velox/archive/${VELOX_REF}.zip")
CPMAddPackage(NAME velox URL ${VELOX_SOURCE_URL} URL_HASH MD5=${VELOX_URL_MD5} OPTIONS "VELOX_MONO_LIBRARY ON" "VELOX_BUILD_TESTING ON")

set(VELOX4J_SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/main)
set(VELOX4J_TEST_DIR ${CMAKE_CURRENT_LIST_DIR}/test)

execute_process(
        COMMAND
        bash -c
        "( source ${velox_SOURCE_DIR}/scripts/setup-helper-functions.sh && echo -n $(get_cxx_flags $ENV{CPU_TARGET}))"
        OUTPUT_VARIABLE SCRIPT_CXX_FLAGS
        RESULT_VARIABLE COMMAND_STATUS)
if (COMMAND_STATUS EQUAL "1")
    message(FATAL_ERROR "Unable to determine compiler flags!")
endif ()
message("Setting CMAKE_CXX_FLAGS=${SCRIPT_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")

add_subdirectory(main)
add_subdirectory(test)

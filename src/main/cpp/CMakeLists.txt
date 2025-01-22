cmake_minimum_required(VERSION 3.29)
project(velox4jcpp)

include(cmake/CPM.cmake)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

set(VELOX_REPO facebookincubator/velox)
set(VELOX_REF 1426473ed52ff0f35e10462f1b0cb9d35fbbc1a0)
set(VELOX_SOURCE_URL "https://github.com/${VELOX_REPO}/archive/${VELOX_REF}.zip")
set(VELOX_SOURCE_URL_MD5 d451b4862eac761d776c034a07a9b2cc)
CPMAddPackage(NAME velox URL ${VELOX_SOURCE_URL} URL_HASH MD5=${VELOX_SOURCE_URL_MD5} OPTIONS "VELOX_MONO_LIBRARY ON" "VELOX_BUILD_TESTING ON")

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

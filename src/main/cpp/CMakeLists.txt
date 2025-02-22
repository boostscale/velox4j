cmake_minimum_required(VERSION 3.28)
project(velox4jcpp)

# Include CPM.cmake.
set(CPM_SOURCE_CACHE ${CMAKE_CURRENT_BINARY_DIR}/.cpm-cache)
include(cmake/CPM.cmake)

# Preconditions.
if (NOT UNIX)
    message(FATAL_ERROR "The project velox4j-cpp only supports UNIX-based systems at the moment.")
endif ()

# Compile options.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

# Velox4j option definitions.
option(VELOX4J_BUILD_TESTING "Build Velox4j tests." ON)

# Velox4j non-bool option definitions.
set(VELOX4J_INSTALL_DESTINATION "lib" CACHE STRING "The directory Velox4j's libraries are installed. This is relative to the value of CMAKE_INSTALL_PREFIX.")

# Setup JNI.
find_package(Java REQUIRED)
find_package(JNI REQUIRED)
if (JNI_FOUND)
    message(STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
    message(STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
endif ()
include(UseJava)

# Set Velox options.
# Workaround for https://github.com/facebookincubator/velox/issues/12358.
unset(BUILD_SHARED_LIBS CACHE)
unset(BUILD_STATIC_LIBS CACHE)
set(VELOX_DEPENDENCY_SOURCE AUTO)
set(VELOX_ENABLE_CCACHE ON)
set(VELOX_MONO_LIBRARY ON)
set(VELOX_BUILD_SHARED ON)
set(VELOX_BUILD_STATIC OFF)
set(VELOX_ENABLE_ARROW ON)
if (NOT VELOX4J_BUILD_TESTING)
    set(VELOX_BUILD_TESTING OFF)
endif ()

# Import Velox.
set(VELOX_PATCH_DIR ${CMAKE_CURRENT_LIST_DIR}/cmake/patches/velox)
set(VELOX_REF_FILE ${CMAKE_CURRENT_LIST_DIR}/velox-ref.txt)
set(VELOX_REF_HASH_FILE ${CMAKE_CURRENT_LIST_DIR}/velox-ref-hash.txt)
set(VELOX_REPO facebookincubator/velox)
file(STRINGS ${VELOX_REF_FILE} VELOX_REF)
file(STRINGS ${VELOX_REF_HASH_FILE} VELOX_SOURCE_URL_MD5)
set(VELOX_SOURCE_URL "https://github.com/${VELOX_REPO}/archive/${VELOX_REF}.zip")
CPMAddPackage(
        NAME velox
        URL ${VELOX_SOURCE_URL}
        URL_HASH MD5=${VELOX_SOURCE_URL_MD5}
        PATCHES
        "${VELOX_PATCH_DIR}/0001-fix-Fix-ambiguous-sources-serde-logic-for-TableWrite.patch"
        "${VELOX_PATCH_DIR}/0002-fix-Change-FetchContent-name-of-gtest-to-googletest-.patch"
        "${VELOX_PATCH_DIR}/0003-feat-Add-JSON-SerDe-code-for-member-field-LocationHa.patch"
)

# Import JniHelpers.
set(JNI_HELPERS_REPO zhztheplayer/JniHelpersCpp)
set(JNI_HELPERS_REF 7c58a9a24c87758fe94f33bec6a4463971af1e23)
set(JNI_HELPERS_SOURCE_URL_MD5 3f168384670e0f21a2e2281f91cfb2e1)
set(JNI_HELPERS_SOURCE_URL "https://github.com/${JNI_HELPERS_REPO}/archive/${JNI_HELPERS_REF}.zip")
CPMAddPackage(NAME jni-helpers URL ${JNI_HELPERS_SOURCE_URL} URL_HASH MD5=${JNI_HELPERS_SOURCE_URL_MD5})
target_include_directories(JniHelpers PRIVATE ${JNI_INCLUDE_DIRS})
target_link_libraries(JniHelpers PRIVATE ${JNI_LIBRARIES})

# Directories of Velox4j.
set(VELOX4J_SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/main)
set(VELOX4J_TEST_DIR ${CMAKE_CURRENT_LIST_DIR}/test)

# Setup compiler flags using Velox's provided script.
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

# Continue to build.
add_subdirectory(main)
if (VELOX4J_BUILD_TESTING)
    add_subdirectory(test)
endif ()

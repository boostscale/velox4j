cmake_minimum_required(VERSION 3.29)
project(velox4jcpp)

set(CMAKE_CXX_STANDARD 20)

include(FetchContent)
list(PREPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake/modules")

find_package(velox REQUIRED)

add_library(velox4j SHARED src/main.cpp)
target_link_libraries(velox4j LINK_PRIVATE velox::velox)
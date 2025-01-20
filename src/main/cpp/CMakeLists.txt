cmake_minimum_required(VERSION 3.29)
project(velox4jcpp)

set(CMAKE_CXX_STANDARD 20)

include(cmake/CPM.cmake)

set(VELOX_REF 54c6d9e966428cf439a9c082613e575c05d75d40)
set(VELOX_URL_MD5 ba8a7c75de609cb04be8817b6760728a)
set(VELOX_SOURCE_URL "https://github.com/facebookincubator/velox/archive/${VELOX_REF}.zip")
CPMAddPackage(NAME velox ${VELOX_SOURCE_URL} URL_HASH MD5=${VELOX_URL_MD5} OPTIONS "VELOX_MONO_LIBRARY ON")

add_library(velox4j SHARED src/main.cpp)
target_link_libraries(velox4j LINK_PRIVATE velox)
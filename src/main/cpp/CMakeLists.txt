cmake_minimum_required(VERSION 3.29)
project(velox4jcpp)

set(CMAKE_CXX_STANDARD 20)

include(cmake/CPM.cmake)

set(VELOX_REF 13e72be62464bf67da925dad764c89753e159299)
set(VELOX_URL_MD5 ebbfec532e1ad3fe6598279da70fe56e)
set(VELOX_SOURCE_URL "https://github.com/zhztheplayer/velox/archive/${VELOX_REF}.zip")
CPMAddPackage(NAME velox URL ${VELOX_SOURCE_URL} URL_HASH MD5=${VELOX_URL_MD5} OPTIONS "VELOX_MONO_LIBRARY ON")

add_library(velox4j SHARED src/main.cpp)
target_link_libraries(velox4j LINK_PRIVATE velox)
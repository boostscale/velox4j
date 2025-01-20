include(FindPackageHandleStandardArgs)
include(SelectLibraryConfigurations)

set(VELOX_REF 54c6d9e966428cf439a9c082613e575c05d75d40)
set(VELOX_URL_MD5 ba8a7c75de609cb04be8817b6760728a)
set(VELOX_SOURCE_URL "https://github.com/facebookincubator/velox/archive/${VELOX_REF}.zip")

set(VELOX_MONO_LIBRARY ON)

message(STATUS "Building velox from source")
FetchContent_Declare(velox URL ${VELOX_SOURCE_URL} URL_HASH MD5=${VELOX_URL_MD5} CMAKE_ARGS -DBUILD_TESTS=OFF -DBUILD_BENCHMARKS=OFF)
FetchContent_MakeAvailable(velox)


message("${VELOX_BUILD_PATH}")
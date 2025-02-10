# Variables.
set(VELOX4J_SOURCES
        velox4j/init/Init.cc
        velox4j/jni/JniCommon.cc
        velox4j/jni/JniError.cc
        velox4j/jni/JniLoader.cc
        velox4j/jni/JniWrapper.cc
        velox4j/jni/StaticJniWrapper.cc
        velox4j/iterator/UpIterator.cc
        velox4j/iterator/DownIterator.cc
        velox4j/lifecycle/ObjectStore.cc
        velox4j/lifecycle/Session.cc
        velox4j/arrow/Arrow.cc
        velox4j/query/Query.cc
        velox4j/query/BoundSplit.cc
        velox4j/query/QueryExecutor.cc
        velox4j/connector/ExternalStream.cc
        velox4j/memory/AllocationListener.cc
        velox4j/memory/JavaAllocationListener.cc
        velox4j/memory/MemoryManager.cc
        velox4j/memory/ArrowMemoryPool.cc
        velox4j/config/Config.cc)
set(VELOX4J_INCLUDES
        ${CMAKE_CURRENT_LIST_DIR}
        ${velox_SOURCE_DIR}
        ${jni-helpers_SOURCE_DIR}
        ${JNI_INCLUDE_DIRS})
set(VELOX4J_DEPENDENCIES
        velox
        JniHelpers
        ${JNI_LIBRARIES})

# Shared library.
add_library(velox4j-shared SHARED ${VELOX4J_SOURCES})
target_include_directories(velox4j-shared PUBLIC ${VELOX4J_INCLUDES})
target_link_libraries(velox4j-shared PUBLIC ${VELOX4J_DEPENDENCIES})
set_target_properties(velox4j-shared PROPERTIES OUTPUT_NAME velox4j-shared LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

# Static library.
add_library(velox4j-static STATIC ${VELOX4J_SOURCES})
target_include_directories(velox4j-static PRIVATE ${VELOX4J_INCLUDES})
target_link_libraries(velox4j-static PRIVATE ${VELOX4J_DEPENDENCIES})
set_target_properties(velox4j-static PROPERTIES OUTPUT_NAME velox4j ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

# Bundled shared library.
add_library(velox4j-bundled SHARED velox4j/Empty.cc)
target_link_libraries(velox4j-bundled PUBLIC velox4j-static)
set_target_properties(velox4j-bundled PROPERTIES OUTPUT_NAME velox4j LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib LINK_FLAGS "-Wl,--whole-archive")

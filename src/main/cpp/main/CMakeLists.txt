add_library(velox4j SHARED
        velox4j/init/Init.cc
        velox4j/jni/JniCommon.cc
        velox4j/jni/JniError.cc
        velox4j/jni/JniLoader.cc
        velox4j/jni/JniWrapper.cc
        velox4j/exec/QueryExecutor.cc
        velox4j/iterator/UpIterator.cc
        velox4j/lifecycle/ObjectStore.cc
        velox4j/lifecycle/Session.cc
        velox4j/arrow/Arrow.cc
        velox4j/query/Query.cc
        velox4j/query/BoundSplit.cc
)

target_include_directories(velox4j PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}
        ${velox_SOURCE_DIR}
        ${jni-helpers_SOURCE_DIR}
        ${JNI_INCLUDE_DIRS}
)

target_link_libraries(velox4j LINK_PUBLIC
        velox
        JniHelpers
        ${JNI_LIBRARIES}
)

set_target_properties(velox4j PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

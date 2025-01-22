add_library(velox4j SHARED
        dummy/Dummy.cc
        init/Init.cc
        jni/JniCommon.cc
        jni/JniError.cc
        jni/JniLoader.cc
        jni/JniWrapper.cc
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
find_package(Java REQUIRED)
find_package(JNI REQUIRED)
if (JNI_FOUND)
    message (STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
    message (STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
endif()
include(UseJava)

add_library(velox4j SHARED
        dummy/Dummy.cc
        init/Init.cc
        jni/JniCommon.cc
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

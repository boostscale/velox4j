add_library(velox4j SHARED
        ${CMAKE_CURRENT_LIST_DIR}/DummySource.cc
        ${CMAKE_CURRENT_LIST_DIR}/init/Init.cc
        ${CMAKE_CURRENT_LIST_DIR}/JniWrapper.cc
)
target_include_directories(velox4j PUBLIC ${velox_SOURCE_DIR})
target_link_libraries(velox4j LINK_PUBLIC velox)

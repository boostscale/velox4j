add_library(velox4j SHARED ${CMAKE_CURRENT_LIST_DIR}/DummySource.cc)
target_include_directories(velox4j PUBLIC ${velox_SOURCE_DIR})
target_link_libraries(velox4j LINK_PUBLIC velox)

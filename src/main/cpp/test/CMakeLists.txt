add_executable(velox4j-test ${CMAKE_CURRENT_LIST_DIR}/DummyTest.cc)
target_include_directories(velox4j-test PRIVATE ${VELOX4J_SOURCE_DIR})
target_link_libraries(velox4j-test velox4j)

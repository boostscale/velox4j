# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
include_guard(GLOBAL)

set(VELOX_CPPKAFKA_VERSION v0.4.1)
set(VELOX_CPPKAFKA_BUILD_SHA256_CHECKSUM
    45770ae0404cb9ba73d659618c51cd55b574c66ed3c7b148360222fb524b0ff7)
set(VELOX_CPPKAFKA_SOURCE_URL
    "https://github.com/mfontanini/cppkafka/archive/refs/tags/v0.4.1.tar.gz")

velox_resolve_dependency_url(CPPKAFKA)

message(STATUS "Building CPPKAFKA from source")
FetchContent_Declare(
  cppkafka
  URL ${VELOX_CPPKAFKA_SOURCE_URL}
  URL_HASH ${VELOX_CPPKAFKA_BUILD_SHA256_CHECKSUM})

FetchContent_MakeAvailable(cppkafka)

if(TARGET cppkafka)
  set_property(
    TARGET cppkafka
    PROPERTY INTERFACE_INCLUDE_DIRECTORIES
             "$<BUILD_INTERFACE:${cppkafka_SOURCE_DIR}/include>"
             "$<BUILD_INTERFACE:${cppkafka_BINARY_DIR}/include>")

  if(NOT TARGET CppKafka::cppkafka)
    add_library(CppKafka::cppkafka ALIAS cppkafka)
  endif()
endif()

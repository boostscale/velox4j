/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <velox/common/memory/Memory.h>
#include <string>
#include "velox4j/iterator/UpIterator.h"
#include "velox4j/memory/MemoryManager.h"

namespace velox4j {

class QueryExecutor {
 public:
  QueryExecutor(MemoryManager* memoryManager, std::string queryJson);

  std::unique_ptr<UpIterator> execute() const;

 private:
  MemoryManager* const memoryManager_;
  const std::string queryJson_;
};

} // namespace velox4j

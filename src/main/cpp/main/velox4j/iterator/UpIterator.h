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

#include <velox/vector/ComplexVector.h>

namespace velox4j {

using namespace facebook::velox;

class UpIterator {
 public:
  // CTOR.
  UpIterator() = default;

  // Delete copy/move CTORs.
  UpIterator(UpIterator&&) = delete;
  UpIterator(const UpIterator&) = delete;
  UpIterator& operator=(const UpIterator&) = delete;
  UpIterator& operator=(UpIterator&&) = delete;

  // DTOR.
  virtual ~UpIterator() = default;

  virtual bool hasNext() = 0;
  virtual RowVectorPtr next() = 0;
};
} // namespace velox4j

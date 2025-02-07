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

#include <velox/connectors/Connector.h>

#pragma once

namespace velox4j {
using namespace facebook::velox;

class ExternalStream {
 public:
  ExternalStream() = default;

  // Delete copy/move CTORs.
  ExternalStream(ExternalStream&&) = delete;
  ExternalStream(const ExternalStream&) = delete;
  ExternalStream& operator=(const ExternalStream&) = delete;
  ExternalStream& operator=(ExternalStream&&) = delete;

  // DTOR.
  virtual ~ExternalStream() = default;

  virtual bool hasNext() = 0;

  virtual RowVectorPtr next() = 0;
};
} // namespace velox4j

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

#include <velox/core/PlanNode.h>
#include <velox/exec/Split.h>

namespace velox4j {

class BoundSplit {
 public:
  BoundSplit(facebook::velox::core::PlanNodeId& planNodeId, std::shared_ptr<facebook::velox::exec::Split>& split)
      : planNodeId_(planNodeId), split_(std::move(split)) {}
  virtual ~BoundSplit() = default;

  const facebook::velox::core::PlanNodeId& planNodeId() const {
    return planNodeId_;
  }

  facebook::velox::exec::Split& split() const {
    return *split_;
  }

 private:
  facebook::velox::core::PlanNodeId planNodeId_;
  std::shared_ptr<facebook::velox::exec::Split> split_;
};
} // namespace velox4j

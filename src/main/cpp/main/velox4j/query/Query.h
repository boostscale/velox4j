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

#include <velox/core/PlanNode.h>
#include <utility>
#include "BoundSplit.h"

#pragma once

namespace velox4j {
using namespace facebook::velox;
class Query : public ISerializable {
 public:
  explicit Query(
      std::shared_ptr<const core::PlanNode>& plan,
      std::vector<std::shared_ptr<BoundSplit>>&& boundSplits);

  const std::shared_ptr<const core::PlanNode>& plan() const;
  const std::vector<std::shared_ptr<BoundSplit>>& boundSplits() const;

  folly::dynamic serialize() const override;

  std::string toString() const;

  static std::shared_ptr<Query> create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe();

 private:
  std::shared_ptr<const core::PlanNode> plan_;
  std::vector<std::shared_ptr<BoundSplit>> boundSplits_;
};
} // namespace velox4j

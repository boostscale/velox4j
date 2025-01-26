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

#include "Query.h"

namespace velox4j {
using namespace facebook::velox;

Query::Query(
    std::shared_ptr<core::PlanNode>& plan,
    std::vector<std::shared_ptr<BoundSplit>>& boundSplits)
    : plan_(plan), boundSplits_(std::move(boundSplits)) {}

const std::shared_ptr<core::PlanNode>& Query::plan() const {
  return plan_;
}

const std::vector<std::shared_ptr<BoundSplit>>& Query::boundSplits() const {
  return boundSplits_;
}

folly::dynamic Query::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "Velox4jQuery";
  obj["plan"] = plan_->serialize();
  obj["boundSplits"] = folly::dynamic::array;
  for (const auto& boundSplit : boundSplits_) {
    folly::dynamic boundSplitObj = folly::dynamic::object;
    boundSplitObj["planNodeId"] = boundSplit->planNodeId();
    boundSplitObj["groupId"] = boundSplit->split().groupId;
    boundSplitObj["split"] = boundSplit->split().connectorSplit->serialize();
    obj["boundSplits"].push_back(boundSplitObj);
  }
  return obj;
}

std::shared_ptr<Query> Query::create(const folly::dynamic& obj, void* context) {
  auto plan = std::const_pointer_cast<core::PlanNode>(
      ISerializable::deserialize<core::PlanNode>(obj["plan"], context));
  std::vector<std::shared_ptr<BoundSplit>> boundSplits{};
  for (const auto& boundSplit : obj["boundSplits"]) {
    auto planNodeId = boundSplit["planNodeId"].asString();
    auto groupId = boundSplit["groupId"].asInt();
    auto connectorSplit = std::const_pointer_cast<connector::ConnectorSplit>(
        ISerializable::deserialize<connector::ConnectorSplit>(
            boundSplit["split"]));
    std::shared_ptr<exec::Split> split = std::make_shared<exec::Split>(
        std::move(connectorSplit), static_cast<int32_t>(groupId));
    boundSplits.push_back(std::make_shared<BoundSplit>(planNodeId, split));
  }
  return std::make_shared<Query>(plan, boundSplits);
}
void Query::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("Velox4jQuery", create);
}
} // namespace velox4j

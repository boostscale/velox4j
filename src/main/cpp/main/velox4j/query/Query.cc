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
    const std::shared_ptr<const core::PlanNode>& plan,
    std::vector<std::shared_ptr<BoundSplit>>&& boundSplits,
    const std::shared_ptr<const ConfigArray>& queryConfig,
    const std::shared_ptr<const ConnectorConfigArray>& connectorConfig)
    : plan_(plan),
      boundSplits_(std::move(boundSplits)),
      queryConfig_(queryConfig),
      connectorConfig_(connectorConfig) {}

const std::shared_ptr<const core::PlanNode>& Query::plan() const {
  return plan_;
}

const std::vector<std::shared_ptr<BoundSplit>>& Query::boundSplits() const {
  return boundSplits_;
}

const std::shared_ptr<const ConfigArray>& Query::queryConfig() const {
  return queryConfig_;
}

const std::shared_ptr<const ConnectorConfigArray>& Query::connectorConfig()
    const {
  return connectorConfig_;
}

std::string Query::toString() const {
  std::vector<std::string> boundSplitStrings{};
  std::transform(
      boundSplits_.begin(),
      boundSplits_.end(),
      boundSplitStrings.begin(),
      [](std::shared_ptr<BoundSplit> s) -> std::string {
        return fmt::format(
            "BoundSplit plan node ID {}, split {}",
            s->planNodeId(),
            s->split().toString());
      });
  return fmt::format(
      "Query: plan {}, splits [{}]",
      plan_->toString(true, true),
      folly::join(",", boundSplitStrings));
}

folly::dynamic Query::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "velox4j.Query";
  obj["plan"] = plan_->serialize();
  folly::dynamic boundSplits = folly::dynamic::array;
  for (const auto& boundSplit : boundSplits_) {
    folly::dynamic boundSplitObj = folly::dynamic::object;
    boundSplitObj["planNodeId"] = boundSplit->planNodeId();
    boundSplitObj["groupId"] = boundSplit->split().groupId;
    boundSplitObj["split"] = boundSplit->split().connectorSplit->serialize();
    boundSplits.push_back(boundSplitObj);
  }
  obj["boundSplits"] = boundSplits;
  obj["queryConfig"] = queryConfig_->serialize();
  obj["connectorConfig"] = connectorConfig_->serialize();
  return obj;
}

std::shared_ptr<Query> Query::create(const folly::dynamic& obj, void* context) {
  auto plan = std::const_pointer_cast<const core::PlanNode>(
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
  auto queryConfig = std::const_pointer_cast<const ConfigArray>(
      ISerializable::deserialize<ConfigArray>(obj["queryConfig"], context));
  auto connectorConfig = std::const_pointer_cast<const ConnectorConfigArray>(
      ISerializable::deserialize<ConnectorConfigArray>(obj["connectorConfig"], context));
  return std::make_shared<Query>(plan, std::move(boundSplits), queryConfig, connectorConfig);
}

void Query::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("velox4j.Query", create);
}
} // namespace velox4j

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
#include <utility>
#include "BoundSplit.h"
#include "velox4j/config/Config.h"

namespace velox4j {
class Query : public facebook::velox::ISerializable {
 public:
  Query(
      const std::shared_ptr<const facebook::velox::core::PlanNode>& plan,
      std::vector<std::shared_ptr<BoundSplit>>&& boundSplits,
      const std::shared_ptr<const ConfigArray>& queryConfig,
      const std::shared_ptr<const ConnectorConfigArray>& connectorConfig);

  const std::shared_ptr<const facebook::velox::core::PlanNode>& plan() const;
  const std::vector<std::shared_ptr<BoundSplit>>& boundSplits() const;
  const std::shared_ptr<const ConfigArray>& queryConfig() const;
  const std::shared_ptr<const ConnectorConfigArray>& connectorConfig() const;

  folly::dynamic serialize() const override;

  std::string toString() const;

  static std::shared_ptr<Query> create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe();

 private:
  const std::shared_ptr<const facebook::velox::core::PlanNode> plan_;
  const std::vector<std::shared_ptr<BoundSplit>> boundSplits_;
  const std::shared_ptr<const ConfigArray> queryConfig_;
  const std::shared_ptr<const ConnectorConfigArray> connectorConfig_;
};
} // namespace velox4j

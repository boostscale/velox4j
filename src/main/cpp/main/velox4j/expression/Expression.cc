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

#include "Expression.h"

namespace velox4j {
using namespace facebook::velox;

Expression::Expression(
    const core::TypedExprPtr& expr,
    const std::shared_ptr<const ConfigArray>& queryConfig,
    const std::shared_ptr<const ConnectorConfigArray>& connectorConfig)
    : expr_(expr),
      queryConfig_(queryConfig),
      connectorConfig_(connectorConfig) {}

const core::TypedExprPtr& Expression::expr() const {
  return expr_;
}

const std::shared_ptr<const ConfigArray>& Expression::queryConfig() const {
  return queryConfig_;
}

const std::shared_ptr<const ConnectorConfigArray>&
Expression::connectorConfig() const {
  return connectorConfig_;
}

folly::dynamic Expression::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "Velox4jExpression";
  obj["expr"] = expr_->serialize();
  obj["queryConfig"] = queryConfig_->serialize();
  obj["connectorConfig"] = connectorConfig_->serialize();
  return obj;
}

std::shared_ptr<Expression> Expression::create(
    const folly::dynamic& obj,
    void* context) {
  auto expr = std::const_pointer_cast<const core::ITypedExpr>(
      ISerializable::deserialize<core::ITypedExpr>(obj["expr"], context));
  auto queryConfig = std::const_pointer_cast<const ConfigArray>(
      ISerializable::deserialize<ConfigArray>(obj["queryConfig"], context));
  auto connectorConfig = std::const_pointer_cast<const ConnectorConfigArray>(
      ISerializable::deserialize<ConnectorConfigArray>(
          obj["connectorConfig"], context));
  return std::make_shared<Expression>(expr, queryConfig, connectorConfig);
}

void Expression::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("Velox4jExpression", create);
}

} // namespace velox4j

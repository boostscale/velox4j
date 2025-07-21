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

#include "velox4j/eval/Evaluator.h"
#include "velox4j/eval/Evaluation.h"

namespace velox4j {
using namespace facebook::velox;
Evaluator::Evaluator(
    MemoryManager* memoryManager,
    const std::shared_ptr<const Evaluation>& evaluation)
    : evaluation_(evaluation) {
  static std::atomic<uint32_t> executionId{0};
  const uint32_t eid = executionId++;
  queryCtx_ = core::QueryCtx::create(
      nullptr,
      core::QueryConfig{evaluation_->queryConfig()->toMap()},
      evaluation_->connectorConfig()->toMap(),
      cache::AsyncDataCache::getInstance(),
      memoryManager
          ->getVeloxPool(
              fmt::format(
                  "Evaluator Memory Pool - EID {}", std::to_string(eid)),
              memory::MemoryPool::Kind::kAggregate)
          ->shared_from_this(),
      nullptr,
      fmt::format("Evaluator Context - EID {}", std::to_string(eid)));
  ee_ = std::make_unique<exec::SimpleExpressionEvaluator>(
      queryCtx_.get(),
      memoryManager->getVeloxPool(
          fmt::format(
              "Evaluator Leaf Memory Pool - EID {}", std::to_string(eid)),
          memory::MemoryPool::Kind::kLeaf));
  exprSet_ = ee_->compile(evaluation_->expr());
}

VectorPtr Evaluator::eval(
    const SelectivityVector& rows,
    const RowVector& input) {
  VectorPtr vector{};
  ee_->evaluate(exprSet_.get(), rows, input, vector);
  VELOX_CHECK_NOT_NULL(vector, "Failed to evaluate expression");
  return vector;
}

} // namespace velox4j

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

#include "QueryExecutor.h"
#include <velox/exec/Task.h>
#include "velox4j/query/Query.h"

#include <utility>

namespace velox4j {

using namespace facebook::velox;

namespace {

class Out : public UpIterator {
 public:
  Out(MemoryManager* memoryManager, std::string queryJson)
      : memoryManager_(memoryManager), queryJson_(std::move(queryJson)) {
    static std::atomic<uint32_t> executionId{
        0}; // Velox query ID, same with taskId.
    const uint32_t eid = executionId++;
    auto querySerdePool = memoryManager_->getVeloxPool(
        fmt::format("Query Serde Memory Pool - EID {}", std::to_string(eid)),
        memory::MemoryPool::Kind::kLeaf);
    // Keep the pool alive until the task is finished.
    auto queryDynamic = folly::parseJson(queryJson_);
    auto query =
        ISerializable::deserialize<Query>(queryDynamic, querySerdePool);
    core::PlanFragment planFragment{
        query->plan(), core::ExecutionStrategy::kUngrouped, 1, {}};
    std::shared_ptr<core::QueryCtx> queryCtx = core::QueryCtx::create(
        nullptr,
        core::QueryConfig{query->queryConfig()->toMap()},
        query->connectorConfig()->toMap(),
        cache::AsyncDataCache::getInstance(),
        memoryManager_
            ->getVeloxPool(
                fmt::format("Query Memory Pool - EID {}", std::to_string(eid)),
                memory::MemoryPool::Kind::kAggregate)
            ->shared_from_this(),
        nullptr,
        fmt::format("Query Context - EID {}", std::to_string(eid)));

    auto task = exec::Task::create(
        fmt::format("Task - EID {}", std::to_string(eid)),
        std::move(planFragment),
        0,
        std::move(queryCtx),
        exec::Task::ExecutionMode::kSerial);

    std::unordered_set<core::PlanNodeId> planNodesWithSplits{};
    for (const auto& boundSplit : query->boundSplits()) {
      exec::Split& split = boundSplit->split();
      planNodesWithSplits.emplace(boundSplit->planNodeId());
      task->addSplit(boundSplit->planNodeId(), std::move(split));
    }
    for (const auto& nodeWithSplits : planNodesWithSplits) {
      task->noMoreSplits(nodeWithSplits);
    }

    task_ = task;

    if (!task_->supportSerialExecutionMode()) {
      VELOX_FAIL(
          "Task doesn't support single threaded execution: " +
          task->toString());
    }
  }

  ~Out() override {
    if (task_ != nullptr && task_->isRunning()) {
      // FIXME: Calling .wait() may take no effect in single thread execution
      //  mode.
      task_->requestCancel().wait();
    }
  }

  bool hasNext() override {
    if (task_->isFinished()) {
      return false;
    }
    if (pending_ != nullptr) {
      return true;
    }
    advance();
    return pending_ != nullptr;
  }

  RowVectorPtr next() override {
    if (!hasNext()) {
      VELOX_FAIL("The iterator is drained");
    }
    auto result = pending_;
    pending_ = nullptr;
    return result;
  }

 private:
  void advance() {
    VELOX_CHECK_NULL(pending_);
    RowVectorPtr vector;
    while (true) {
      auto future = ContinueFuture::makeEmpty();
      auto out = task_->next(&future);
      if (!future.valid()) {
        // Not need to wait. Break.
        vector = std::move(out);
        break;
      }
      // Velox suggested to wait. This might be because another thread (e.g.,
      // background io thread) is spilling the task.
      VELOX_CHECK_NULL(
          out,
          "Expected to wait but still got non-null output from Velox task");
      VLOG(2)
          << "Velox task " << task_->taskId()
          << " is busy when ::next() is called. Will wait and try again. Task state: "
          << taskStateString(task_->state());
      future.wait();
    }
    pending_ = vector;
  }

  MemoryManager* const memoryManager_;
  const std::string queryJson_;
  std::shared_ptr<exec::Task> task_;
  RowVectorPtr pending_;
};
} // namespace

QueryExecutor::QueryExecutor(MemoryManager* memoryManager, std::string planJson)
    : memoryManager_(memoryManager), queryJson_(std::move(planJson)) {}

std::unique_ptr<UpIterator> QueryExecutor::execute() const {
  return std::make_unique<Out>(memoryManager_, queryJson_);
}
} // namespace velox4j

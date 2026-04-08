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

#include "StatefulQueryExecutor.h"
#include <velox/experimental/stateful/state/StateBackend.h>
#include <velox/experimental/stateful/state/RocksDBStateBackend.h>
#include "velox4j/query/Query.h"
#include <string>
#include <utility>
#include <folly/json.h>
#include <folly/json/dynamic.h>
#include <folly/json/json.h>

namespace velox4j {

using namespace facebook::velox;

StatefulSerialTask::StatefulSerialTask(
    MemoryManager* memoryManager,
    std::shared_ptr<const Query> query)
    : memoryManager_(memoryManager), query_(std::move(query)) {
  static std::atomic<uint32_t> executionId{0};
  const uint32_t eid = executionId++;
  auto connectorConfigs = query_->connectorConfig()->toMap();
  std::string taskIndex = std::to_string(eid);
  for (const auto &[key, config] : connectorConfigs) {
    taskIndex = config->get<std::string>("task_index", taskIndex);
    break;
  }
  core::PlanFragment planFragment{
      query_->plan(), core::ExecutionStrategy::kUngrouped, 1, {}};
  std::shared_ptr<core::QueryCtx> queryCtx = core::QueryCtx::create(
      nullptr,
      core::QueryConfig{query_->queryConfig()->toMap()},
      query_->connectorConfig()->toMap(),
      cache::AsyncDataCache::getInstance(),
      memoryManager_
          ->getVeloxPool(
              fmt::format("Query Memory Pool - EID {}", taskIndex),
              memory::MemoryPool::Kind::kAggregate)
          ->shared_from_this(),
      nullptr,
      fmt::format("Query Context - EID {}", taskIndex));

  auto task = stateful::StatefulTask::create(
      fmt::format("Task - EID {}", taskIndex),
      std::move(planFragment),
      std::move(queryCtx));

  task_ = task;
  task_->init();
}

StatefulSerialTask::~StatefulSerialTask() {
  if (task_ != nullptr && task_->isRunning()) {
    // TODO: add a method to finish the task and set state.
    task_->finish();
    // FIXME: Calling .wait() may take no effect in single thread execution
    //  mode.
    task_->requestCancel().wait();
  }
  task_.reset();
}

UpIterator::State StatefulSerialTask::advance() {
  VELOX_CHECK_NULL(pending_);
  return advance0(false);
}

void StatefulSerialTask::wait() {
}

RowVectorPtr StatefulSerialTask::get() {
  VELOX_CHECK(false, "Should not call get for stateful task.");
  return nullptr;
}

stateful::StreamElementPtr StatefulSerialTask::statefulGet() {
  VELOX_CHECK_NOT_NULL(
      pending_,
      "SerialTask: No pending row vector to return. Make sure the "
      "iterator is available via member function advance() first");
  const auto out = std::move(pending_);
  pending_ = nullptr;
  return out;
}

void StatefulSerialTask::notifyWatermark(long watermark, int index) {
  task_->notifyWatermark(watermark, index);
}

void StatefulSerialTask::notifyWatermark(long watermark) {
  task_->notifyWatermark(watermark);
}

void StatefulSerialTask::initializeState(long checkpointId, std::string keyedStateBackendConfigString) {
  folly::dynamic obj = folly::parseJson(keyedStateBackendConfigString);
  std::shared_ptr<const stateful::KeyedStateBackendParameters> params = stateful::KeyedStateBackendParameters::create(obj, nullptr);
  if (params && params->getBackendType() == stateful::StateBackendType::ROCKSDB) {
    auto rocksdbParams = stateful::RocksDBKeyedStateBackendParameters::create(obj, nullptr);
    task_->initializeState(rocksdbParams);
  } else {
    // params maybe null, then initialize by using default heap state backend.
    task_->initializeState(params);
  }
}

void StatefulSerialTask::snapshotState(long checkpointId) {
  task_->snapshotState();
}

std::vector<std::string> StatefulSerialTask::notifyCheckpointComplete(long checkpointId) {
  return task_->notifyCheckpointComplete(checkpointId);
}

void StatefulSerialTask::notifyCheckpointAborted(long checkpointId) {
  task_->notifyCheckpointAborted(checkpointId);
}

void StatefulSerialTask::addSplit(
    const core::PlanNodeId& planNodeId,
    int32_t groupId,
    std::shared_ptr<connector::ConnectorSplit> connectorSplit) {
  auto cs = connectorSplit;
  task_->addSplit(planNodeId, exec::Split{std::move(cs), groupId});
}

void StatefulSerialTask::noMoreSplits(const core::PlanNodeId& planNodeId) {
  task_->noMoreSplits(planNodeId);
}

std::unique_ptr<SerialTaskStats> StatefulSerialTask::collectStats() {
  const auto stats = task_->statefulTaskStats();
  return std::make_unique<SerialTaskStats>(stats);
}

UpIterator::State StatefulSerialTask::advance0(bool wait) {
  while (true) {
    int32_t retCode = 0;
    auto out = task_->next(retCode);
    if (out != nullptr) {
      pending_ = std::move(out);
      return State::AVAILABLE;
    }
    if (retCode == 1) {
      return State::FINISHED;
    }
    return State::BLOCKED;
  }
}

StatefulQueryExecutor::StatefulQueryExecutor(
    MemoryManager* memoryManager,
    std::shared_ptr<const Query> query)
    : memoryManager_(memoryManager), query_(query) {}

std::unique_ptr<StatefulSerialTask> StatefulQueryExecutor::execute() const {
  return std::make_unique<StatefulSerialTask>(memoryManager_, query_);
}

// void StatefulSerialTask::onProcessingTime(int64_t key, int64_t ns, int64_t timestamp) {
//   task_->onProcessingTime(key, ns, timestamp);
// }

// void StatefulSerialTask::onEventTime(int64_t key, int64_t ns, int64_t timestamp) {
//   task_->onEventTime(key, ns, timestamp);
// }

} // namespace velox4j

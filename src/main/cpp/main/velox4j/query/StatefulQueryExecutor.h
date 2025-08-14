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

#include <string>
#include "Query.h"
#include <velox/experimental/stateful/StatefulTask.h>
#include <velox/experimental/stateful/StreamElement.h>
#include "velox4j/iterator/UpIterator.h"
#include "velox4j/memory/MemoryManager.h"
#include "velox4j/query/QueryExecutor.h"

namespace velox4j {

class StatefulSerialTask : public UpIterator {
 public:
  StatefulSerialTask(MemoryManager* memoryManager, std::shared_ptr<const Query> query);

  ~StatefulSerialTask() override;

  State advance() override;

  void wait() override;

  facebook::velox::RowVectorPtr get() override;

  facebook::velox::stateful::StreamElementPtr statefulGet();

  void notifyWatermark(long watermark, int index);

  void addSplit(
      const facebook::velox::core::PlanNodeId& planNodeId,
      int32_t groupId,
      std::shared_ptr<facebook::velox::connector::ConnectorSplit>
          connectorSplit);

  void noMoreSplits(const facebook::velox::core::PlanNodeId& planNodeId);

  std::unique_ptr<SerialTaskStats> collectStats();

 private:
  State advance0(bool wait);

  MemoryManager* const memoryManager_;
  std::shared_ptr<const Query> query_;
  std::shared_ptr<facebook::velox::stateful::StatefulTask> task_;
  facebook::velox::stateful::StreamElementPtr pending_{nullptr};
  std::string outNodeId_;
};

class StatefulQueryExecutor {
 public:
  StatefulQueryExecutor(
      MemoryManager* memoryManager,
      std::shared_ptr<const Query> query);

  std::unique_ptr<StatefulSerialTask> execute() const;

 private:
  MemoryManager* const memoryManager_;
  const std::shared_ptr<const Query> query_;
};

} // namespace velox4j

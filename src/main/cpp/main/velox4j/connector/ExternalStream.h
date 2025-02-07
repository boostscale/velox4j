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

#include <velox/connectors/Connector.h>
#include <velox/exec/Driver.h>
#include <velox/exec/Task.h>
#include "velox4j/lifecycle/ObjectStore.h"

#pragma once

namespace velox4j {
using namespace facebook::velox;

class SuspendedSection {
 public:
  explicit SuspendedSection(facebook::velox::exec::Driver* driver)
      : driver_(driver) {
    if (driver_->task()->enterSuspended(driver_->state()) !=
        facebook::velox::exec::StopReason::kNone) {
      VELOX_FAIL(
          "Terminate detected when entering suspended section for driver {} from task {}",
          driver_->driverCtx()->driverId,
          driver_->task()->taskId());
    }
  }

  virtual ~SuspendedSection() {
    if (driver_->task()->leaveSuspended(driver_->state()) !=
        facebook::velox::exec::StopReason::kNone) {
      LOG(WARNING)
          << "Terminate detected when leaving suspended section for driver "
          << driver_->driverCtx()->driverId << " from task "
          << driver_->task()->taskId();
    }
  }

 private:
  facebook::velox::exec::Driver* const driver_;
};

class ExternalStream {
 public:
  ExternalStream() = default;

  // Delete copy/move CTORs.
  ExternalStream(ExternalStream&&) = delete;
  ExternalStream(const ExternalStream&) = delete;
  ExternalStream& operator=(const ExternalStream&) = delete;
  ExternalStream& operator=(ExternalStream&&) = delete;

  // DTOR.
  virtual ~ExternalStream() = default;

  virtual bool hasNext() = 0;

  virtual RowVectorPtr next() = 0;
};

class ExternalStreamConnectorSplit : public connector::ConnectorSplit {
 public:
  ExternalStreamConnectorSplit(const std::string& connectorId, ObjectHandle esId)
      : ConnectorSplit(connectorId), esId_(esId) {}

  const ObjectHandle esId() const {
    return esId_;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "ExternalStreamConnectorSplit";
    obj["connectorId"] = connectorId;
    obj["esId"] = esId_;
    return obj;
  }

  static void registerSerDe() {
    auto& registry = DeserializationWithContextRegistryForSharedPtr();
    registry.Register("ExternalStreamConnectorSplit", create);
  }

  static std::shared_ptr<ExternalStreamConnectorSplit> create(
      const folly::dynamic& obj,
      void* context) {
    const auto connectorId = obj["connectorId"].asString();
    const auto esId = obj["esId"].asInt();
    return std::make_shared<ExternalStreamConnectorSplit>(connectorId, esId);
  }

 private:
  const ObjectHandle esId_;
};

class ExternalStreamTableHandle : public connector::ConnectorTableHandle {
 public:
  explicit ExternalStreamTableHandle(const std::string& connectorId)
      : ConnectorTableHandle(connectorId) {}

  folly::dynamic serialize() const override {
    folly::dynamic obj =
        ConnectorTableHandle::serializeBase("ExternalStreamTableHandle");
    return obj;
  }

  static void registerSerDe() {
    auto& registry = DeserializationWithContextRegistryForSharedPtr();
    registry.Register("ExternalStreamTableHandle", create);
  }

  static connector::ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context) {
    auto connectorId = obj["connectorId"].asString();
    return std::make_shared<const ExternalStreamTableHandle>(connectorId);
  }
};

class ExternalStreamDataSource : public connector::DataSource {
 public:
  explicit ExternalStreamDataSource(
      std::shared_ptr<connector::ConnectorTableHandle> tableHandle)
      : DataSource() {
    tableHandle_ =
        std::dynamic_pointer_cast<ExternalStreamTableHandle>(tableHandle);
  }

  void addSplit(std::shared_ptr<connector::ConnectorSplit> split) override {
    VELOX_CHECK(
        split->connectorId == tableHandle_->connectorId(),
        "Split's connector ID doesn't match table handle's connector ID");
    auto esSplit = std::dynamic_pointer_cast<ExternalStreamConnectorSplit>(split);
    auto es = ObjectStore::retrieve<ExternalStream>(esSplit->esId());
    streams_.push(es);
  }

  std::optional<RowVectorPtr> next(uint64_t size, ContinueFuture& future)
      override {
    while (true) {
      if (current_ == nullptr) {
        if (streams_.empty()) {
          // End of all streams.
          return nullptr;
        }
        current_ = streams_.front();
        streams_.pop();
        continue;
      }
      bool hasNext;
      {
        // We are leaving Velox task execution and are entering external code.
        // Suspend the current driver to make the current task open to spilling.
        //
        // When a task is getting spilled, it should have been suspended so has
        // zero running threads, otherwise there's possibility that this spill
        // call hangs. See
        // https://github.com/apache/incubator-gluten/issues/7243. As of now,
        // non-zero running threads usually happens when:
        // 1. Task A spills task B;
        // 2. Task A tries to grow buffers created by task B, during which spill
        // is requested on task A again.
        const exec::DriverThreadContext* driverThreadCtx =
            exec::driverThreadContext();
        VELOX_CHECK_NOT_NULL(
            driverThreadCtx,
            "ExternalStreamDataSource::next() is not called from a driver thread");
        SuspendedSection ss(driverThreadCtx->driverCtx()->driver);
        hasNext = current_->hasNext();
      }
      if (!hasNext) {
        // End of the current stream.
        current_ = nullptr;
        continue;
      }
      RowVectorPtr vector;
      {
        // The same reason as above of the suspension.
        const exec::DriverThreadContext* driverThreadCtx =
            exec::driverThreadContext();
        VELOX_CHECK_NOT_NULL(
            driverThreadCtx,
            "ExternalStreamDataSource::next() is not called from a driver thread");
        SuspendedSection ss(driverThreadCtx->driverCtx()->driver);
        vector = current_->next();
      }
      VELOX_CHECK_NOT_NULL(vector);
      return vector;
    }
  }

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override {
    // TODO.
    VELOX_NYI();
  }

  uint64_t getCompletedBytes() override {
    // TODO.
    return 0;
  }

  uint64_t getCompletedRows() override {
    // TODO.
    return 0;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    // TODO.
    return {};
  }

 private:
  std::shared_ptr<ExternalStreamTableHandle> tableHandle_;
  std::queue<std::shared_ptr<ExternalStream>> streams_{};
  std::shared_ptr<ExternalStream> current_{nullptr};
};

class ExternalStreamConnector : public connector::Connector {
 public:
  ExternalStreamConnector(
      const std::string& id,
      const std::shared_ptr<const config::ConfigBase>& config)
      : connector::Connector(id), config_(config) {}

  std::unique_ptr<connector::DataSource> createDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      connector::ConnectorQueryCtx* connectorQueryCtx) override {
    VELOX_CHECK(
        columnHandles.empty(),
        "ExternalStreamConnector doesn't accept column handles");
    return std::unique_ptr<ExternalStreamDataSource>();
  }

  std::unique_ptr<connector::DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<connector::ConnectorInsertTableHandle>
          connectorInsertTableHandle,
      connector::ConnectorQueryCtx* connectorQueryCtx,
      connector::CommitStrategy commitStrategy) override {
    VELOX_NYI();
  }

 private:
  std::shared_ptr<const config::ConfigBase> config_;
};

class ExternalStreamConnectorFactory : public connector::ConnectorFactory {
 public:
  static constexpr const char* kConnectorName = "external-stream";

  ExternalStreamConnectorFactory() : ConnectorFactory(kConnectorName) {}

  std::shared_ptr<connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* ioExecutor,
      folly::Executor* cpuExecutor) override {
    return std::make_shared<ExternalStreamConnector>(id, config);
  }
};

} // namespace velox4j

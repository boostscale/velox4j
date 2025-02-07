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
  explicit SuspendedSection(facebook::velox::exec::Driver* driver);

  virtual ~SuspendedSection();

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
  ExternalStreamConnectorSplit(
      const std::string& connectorId,
      ObjectHandle esId);

  const ObjectHandle esId() const;

  folly::dynamic serialize() const override;

  static void registerSerDe();

  static std::shared_ptr<ExternalStreamConnectorSplit> create(
      const folly::dynamic& obj,
      void* context);

 private:
  const ObjectHandle esId_;
};

class ExternalStreamTableHandle : public connector::ConnectorTableHandle {
 public:
  explicit ExternalStreamTableHandle(const std::string& connectorId);

  folly::dynamic serialize() const override;

  static void registerSerDe();

  static connector::ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);
};

class ExternalStreamDataSource : public connector::DataSource {
 public:
  explicit ExternalStreamDataSource(
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle);

  void addSplit(std::shared_ptr<connector::ConnectorSplit> split) override;

  std::optional<RowVectorPtr> next(uint64_t size, ContinueFuture& future)
      override;

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
      const std::shared_ptr<const config::ConfigBase>& config);

  std::unique_ptr<connector::DataSource> createDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      connector::ConnectorQueryCtx* connectorQueryCtx) override;

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

  ExternalStreamConnectorFactory();

  std::shared_ptr<connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* ioExecutor,
      folly::Executor* cpuExecutor) override;
};

} // namespace velox4j

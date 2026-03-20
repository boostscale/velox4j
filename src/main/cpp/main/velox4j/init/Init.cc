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

#include "Init.h"
#include <velox/common/memory/Memory.h>
#include <velox/connectors/fuzzer/DiscardDataSink.cpp>
#include <velox/connectors/fuzzer/FuzzerConnector.cpp>
#include <velox/connectors/fuzzer/FuzzerConnectorSplit.h>
#include <velox/connectors/hive/HiveConnector.h>
#include <velox/connectors/hive/HiveConnectorSplit.h>
#include <velox/connectors/hive/HiveDataSink.h>
#include <velox/connectors/nexmark/NexmarkConnector.h>
#include <velox/connectors/nexmark/NexmarkConnectorSplit.h>
#include <velox/connectors/kafka/KafkaConnector.h>
#include <velox/connectors/kafka/KafkaConnectorSplit.h>
#include <velox/connectors/kafka/KafkaTableHandle.h>
#include <velox/connectors/print/PrintConnector.h>
#include <velox/connectors/print/PrintTableHandle.h>
#include <velox/connectors/from_elements/FromElementsConnector.h>
#include <velox/connectors/from_elements/FromElementsTableHandle.h>
#include <velox/connectors/from_elements/FromElementsConnectorSplit.h>
#include <velox/connectors/filesystem/FileSystemInsertTableHandle.h>
#include <velox/connectors/filesystem/FileSystemConnector.h>
#include <velox/dwio/parquet/RegisterParquetReader.h>
#include <velox/dwio/parquet/RegisterParquetWriter.h>
#include <velox/dwio/text/RegisterTextWriter.h>
#include <velox/exec/PartitionFunction.h>
#include <velox/experimental/stateful/StatefulPlanNode.h>
#include <velox/experimental/stateful/state/StateBackend.h>
#include <velox/experimental/stateful/state/RocksDBStateBackend.h>
#include <velox/experimental/stateful/InternalTimerService.h>
#include <velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h>
#include <velox/functions/prestosql/window/WindowFunctionsRegistration.h>
#include <velox/functions/sparksql/aggregates/Register.h>
#include <velox/functions/sparksql/registration/Register.h>
#include <velox/functions/sparksql/window/WindowFunctionsRegistration.h>
#include <velox/vector/fuzzer/ConstrainedVectorGenerator.cpp>
#include <velox/vector/fuzzer/Utils.cpp>
#include <velox/vector/fuzzer/VectorFuzzer.cpp>
#include <velox/experimental/stateful/udf/Register.h>
#include "velox4j/config/Config.h"
#include "velox4j/connector/ExternalStream.h"
#include "velox4j/eval/Evaluation.h"
#include "velox4j/init/Config.h"
#include "velox4j/query/Query.h"
#include "velox4j/jni/StatefulTimerService.h"

namespace velox4j {

using namespace facebook::velox;

namespace {
void init(const std::function<void()>& f) {
  static std::atomic<bool> initialized{false};
  bool expected = false;
  if (!initialized.compare_exchange_strong(expected, true)) {
    VELOX_FAIL("Velox4J was already initialized");
  }
  f();
}

void initForSpark() {
  FLAGS_velox_memory_leak_check_enabled = true;
  FLAGS_velox_memory_pool_capacity_transfer_across_tasks = true;
  FLAGS_velox_exception_user_stacktrace_enabled = true;
  FLAGS_velox_exception_system_stacktrace_enabled = true;
  filesystems::registerLocalFileSystem();
  memory::MemoryManager::initialize({});
  dwio::common::registerFileSinks();
  parquet::registerParquetReaderFactory();
  parquet::registerParquetWriterFactory();
  text::registerTextWriterFactory();
  functions::sparksql::registerFunctions();
  aggregate::prestosql::registerAllAggregateFunctions(
      "",
      true /*registerCompanionFunctions*/,
      false /*onlyPrestoSignatures*/,
      true /*overwrite*/);
  functions::aggregate::sparksql::registerAggregateFunctions(
      "", true /*registerCompanionFunctions*/, true /*overwrite*/);
  window::prestosql::registerAllWindowFunctions();
  functions::window::sparksql::registerWindowFunctions("");
  stateful::udf::registerFunctions();

  ConfigArray::registerSerDe();
  ConnectorConfigArray::registerSerDe();
  Evaluation::registerSerDe();
  Query::registerSerDe();
  Type::registerSerDe();
  common::Filter::registerSerDe();
  connector::fuzzer::DiscardDataTableHandle::registerSerDe();
  connector::fuzzer::FuzzerTableHandle::registerSerDe();
  connector::fuzzer::FuzzerConnectorSplit::registerSerDe();
  connector::hive::HiveTableHandle::registerSerDe();
  connector::hive::LocationHandle::registerSerDe();
  connector::hive::HiveColumnHandle::registerSerDe();
  connector::hive::HiveConnectorSplit::registerSerDe();
  connector::hive::registerHivePartitionFunctionSerDe();
  connector::hive::HiveInsertTableHandle::registerSerDe();
  connector::hive::LocationHandle::registerSerDe();
  connector::hive::HiveSortingColumn::registerSerDe();
  connector::hive::HiveBucketProperty::registerSerDe();
  connector::hive::HiveInsertFileNameGenerator::registerSerDe();
  connector::registerConnector(std::make_shared<connector::hive::HiveConnector>(
      "connector-hive",
      std::make_shared<facebook::velox::config::ConfigBase>(
          std::unordered_map<std::string, std::string>()),
      nullptr));
  connector::kafka::KafkaTableHandle::registerSerDe();
  connector::kafka::KafkaConnectorSplit::registerSerDe();
  connector::registerConnector(std::make_shared<connector::kafka::KafkaConnector>(
      "connector-kafka",
      std::make_shared<facebook::velox::config::ConfigBase>(
        std::unordered_map<std::string, std::string>()),
      nullptr));
  connector::filesystem::FileSystemInsertTableHandle::registerSerDe();
  connector::registerConnector(std::make_shared<connector::filesystem::FileSystemConnector>(
      "connector-filesystem",
      std::make_shared<facebook::velox::config::ConfigBase>(
        std::unordered_map<std::string, std::string>()),
      nullptr));
  ExternalStreamConnectorSplit::registerSerDe();
  ExternalStreamTableHandle::registerSerDe();
  connector::registerConnector(std::make_shared<ExternalStreamConnector>(
      "connector-external-stream",
      std::make_shared<facebook::velox::config::ConfigBase>(
          std::unordered_map<std::string, std::string>())));
  connector::registerConnector(std::make_shared<connector::fuzzer::FuzzerConnector>(
      "connector-fuzzer",
      std::make_shared<facebook::velox::config::ConfigBase>(
          std::unordered_map<std::string, std::string>()),
      nullptr));
  connector::nexmark::NexmarkTableHandle::registerSerDe();
  connector::nexmark::NexmarkConnectorSplit::registerSerDe();
  connector::registerConnector(std::make_shared<connector::nexmark::NexmarkConnector>(
      "connector-nexmark",
      std::make_shared<facebook::velox::config::ConfigBase>(
          std::unordered_map<std::string, std::string>()),
      nullptr));
  connector::print::PrintTableHandle::registerSerDe();
  connector::registerConnector(std::make_shared<connector::print::PrintConnector>(
      "connector-print",
      std::make_shared<facebook::velox::config::ConfigBase>(
          std::unordered_map<std::string, std::string>()),
        nullptr
      ));
  connector::from_elements::FromElementsTableHandle::registerSerDe();
  connector::from_elements::FromElementsConnectorSplit::registerSerDe();
  connector::registerConnector(std::make_shared<connector::from_elements::FromElementsConnector>(
      "connector-from-elements",
      std::make_shared<facebook::velox::config::ConfigBase>(
          std::unordered_map<std::string, std::string>()),
        nullptr
      ));
  core::PlanNode::registerSerDe();
  stateful::StatefulPlanNode::registerSerDe();
  stateful::KeyedStateBackendParameters::registerSerDe();
  stateful::RocksDBKeyedStateBackendParameters::registerSerDe();
  stateful::registerTimerService<int64_t, int64_t>("stateful-timer-service",
    std::make_shared<velox4j::StatefulTimerService<int64_t, int64_t>>("stateful-timer-service"));
  core::ITypedExpr::registerSerDe();
  exec::registerPartitionFunctionSerDe();
}
} // namespace

void initialize(const std::shared_ptr<ConfigArray>& configArray) {
  init([&]() -> void {
    FLAGS_logtostderr = true;
    google::InitGoogleLogging("velox");
    auto vConfig = std::make_shared<facebook::velox::config::ConfigBase>(
        configArray->toMap());
    auto preset = vConfig->get(VELOX4J_INIT_PRESET);
    switch (preset) {
      case SPARK:
        initForSpark();
        break;
      default:
        VELOX_FAIL("Unknown preset: {}", folly::to<std::string>(preset));
    }
  });
}
} // namespace velox4j

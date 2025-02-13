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
#include <velox/connectors/hive/HiveConnector.h>
#include <velox/connectors/hive/HiveConnectorSplit.h>
#include <velox/connectors/hive/HiveDataSink.h>
#include <velox/connectors/hive/TableHandle.h>
#include <velox/core/PlanNode.h>
#include <velox/dwio/parquet/RegisterParquetReader.h>
#include <velox/dwio/parquet/RegisterParquetWriter.h>
#include <velox/exec/PartitionFunction.h>
#include <velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h>
#include <velox/functions/prestosql/window/WindowFunctionsRegistration.h>
#include <velox/functions/sparksql/aggregates/Register.h>
#include <velox/functions/sparksql/registration/Register.h>
#include <velox/functions/sparksql/window/WindowFunctionsRegistration.h>
#include <velox/type/Filter.h>
#include "velox4j/config/Config.h"
#include "velox4j/connector/ExternalStream.h"
#include "velox4j/query/Query.h"

namespace velox4j {

using namespace facebook::velox;

namespace {
void init(const std::function<void()>& f) {
  static std::atomic<bool> initialized{false};
  bool expected = false;
  if (!initialized.compare_exchange_strong(expected, true)) {
    VELOX_FAIL("Velox4j was already initialized");
  }
  f();
}
} // namespace

void initForSpark() {
  init([]() -> void {
    config::globalConfig().memoryLeakCheckEnabled = true;
    config::globalConfig().memoryPoolCapacityTransferAcrossTasks = true;
    config::globalConfig().exceptionSystemStacktraceEnabled = true;
    config::globalConfig().exceptionUserStacktraceEnabled = true;
    filesystems::registerLocalFileSystem();
    memory::MemoryManager::initialize({});
    dwio::common::registerFileSinks();
    parquet::registerParquetReaderFactory();
    parquet::registerParquetWriterFactory();
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

    ConfigArray::registerSerDe();
    ConnectorConfigArray::registerSerDe();
    Query::registerSerDe();
    Type::registerSerDe();
    common::Filter::registerSerDe();
    connector::hive::HiveTableHandle::registerSerDe();
    connector::hive::LocationHandle::registerSerDe();
    connector::hive::HiveColumnHandle::registerSerDe();
    connector::hive::HiveInsertTableHandle::registerSerDe();
    connector::hive::HiveConnectorSplit::registerSerDe();
    connector::hive::registerHivePartitionFunctionSerDe();
    connector::registerConnector(
        std::make_shared<connector::hive::HiveConnector>(
            "connector-hive",
            std::make_shared<facebook::velox::config::ConfigBase>(
                std::unordered_map<std::string, std::string>()),
            nullptr));
    ExternalStreamConnectorSplit::registerSerDe();
    ExternalStreamTableHandle::registerSerDe();
    connector::registerConnector(std::make_shared<ExternalStreamConnector>(
        "connector-external-stream",
        std::make_shared<facebook::velox::config::ConfigBase>(
            std::unordered_map<std::string, std::string>())));
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    exec::registerPartitionFunctionSerDe();
  });
}
} // namespace velox4j

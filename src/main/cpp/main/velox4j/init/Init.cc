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
#include <velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h>
#include <velox/functions/prestosql/window/WindowFunctionsRegistration.h>
#include <velox/functions/sparksql/aggregates/Register.h>
#include <velox/functions/sparksql/registration/Register.h>
#include <velox/functions/sparksql/window/WindowFunctionsRegistration.h>

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
    config::globalConfig.exceptionSystemStacktraceEnabled = true;
    config::globalConfig.exceptionUserStacktraceEnabled = true;
    memory::MemoryManager::initialize({});
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
  });
}
} // namespace velox4j

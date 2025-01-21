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

#include "Dummy.h"
#include <iostream>
#include "init/Init.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/sparksql/aggregates/Register.h"
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace velox4j {
class VectorTest : public facebook::velox::test::VectorTestBase {
 public:
  void foo() {
    std::vector<RowVectorPtr> data{makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3}),
        makeFlatVector<int32_t>({10, 20, 30}),
        makeConstant(true, 3),
    })};

    auto plan = PlanBuilder()
                    .values({data})
                    .partialAggregation({"c0"}, {"count(1)", "sum(c1)"})
                    .finalAggregation()
                    .planNode();
    std::cout << folly::toPrettyJson(plan->serialize()) << std::endl;
  }
};

void foo() {
  memory::MemoryManager::initialize({});
  functions::sparksql::registerFunctions();
  aggregate::prestosql::registerAllAggregateFunctions(
      "",
      true /*registerCompanionFunctions*/,
      false /*onlyPrestoSignatures*/,
      true /*overwrite*/);
  functions::aggregate::sparksql::registerAggregateFunctions(
      "", true /*registerCompanionFunctions*/, true /*overwrite*/);
  auto vector =
      BaseVector::create(VARCHAR(), 100, memory::memoryManager()->spillPool());
  std::cout << "Hello, World! " << vector->toString() << std::endl;

  VectorTest vectorTest;
  vectorTest.foo();
}

void foo1() {
  velox4j::initForSpark();
  auto vector =
      BaseVector::create(VARCHAR(), 100, memory::memoryManager()->spillPool());
  std::cout << "Hello, World! " << vector->toString() << std::endl;

  VectorTest vectorTest;
  vectorTest.foo();
}
} // namespace velox4j

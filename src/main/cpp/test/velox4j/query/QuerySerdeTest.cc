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

#include <gtest/gtest.h>
#include <velox/exec/tests/utils/HiveConnectorTestBase.h>
#include <velox/exec/tests/utils/PlanBuilder.h>
#include "velox4j/test/Init.h"
#include "velox4j/query/Query.h"

namespace velox4j {
using namespace facebook::velox;
using namespace facebook::velox::exec::test;

class QuerySerdeTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    testingEnsureInitializedForSpark();
  }

  QuerySerdeTest() {
    data_ = {makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3}),
        makeFlatVector<int32_t>({10, 20, 30}),
        makeConstant(true, 3),
    })};
  }

  std::vector<RowVectorPtr> data_;
};

TEST_F(QuerySerdeTest, sanity) {
  auto plan = PlanBuilder()
                  .values({data_})
                  .partialAggregation({"c0"}, {"count(1)", "sum(c1)"})
                  .finalAggregation()
                  .planNode();
  std::vector<std::shared_ptr<BoundSplit>> boundSplits{};
  auto query = std::make_shared<Query>(plan, std::move(boundSplits));
}
} // namespace velox4j

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include <velox/exec/WindowFunction.h>
#include <velox/type/Type.h>
#include <velox4j/test/Init.h>

namespace velox4j {
using namespace facebook::velox;

class InitFlinkTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    testingEnsureInitializedForFlink();
  }
};

TEST_F(InitFlinkTest, rankingFunctionsResolveToBigint) {
  EXPECT_EQ(
      exec::resolveWindowResultType("row_number", {})->kind(),
      TypeKind::BIGINT);
  EXPECT_EQ(
      exec::resolveWindowResultType("rank", {})->kind(), TypeKind::BIGINT);
  EXPECT_EQ(
      exec::resolveWindowResultType("dense_rank", {})->kind(),
      TypeKind::BIGINT);
}
} // namespace velox4j

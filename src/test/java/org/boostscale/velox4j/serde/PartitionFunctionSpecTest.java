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
package org.boostscale.velox4j.serde;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.boostscale.velox4j.plan.partition.GatherPartitionFunctionSpec;
import org.boostscale.velox4j.plan.partition.HashPartitionFunctionSpec;
import org.boostscale.velox4j.plan.partition.RoundRobinPartitionFunctionSpec;
import org.boostscale.velox4j.test.Velox4jTests;
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.RowType;

public class PartitionFunctionSpecTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
  }

  @Test
  public void testGatherPartitionFunctionSpec() {
    SerdeTests.testISerializableRoundTrip(new GatherPartitionFunctionSpec());
  }

  @Test
  public void testHashPartitionFunctionSpec() {
    final RowType inputType =
        new RowType(
            ImmutableList.of("foo", "bar"), ImmutableList.of(new IntegerType(), new IntegerType()));
    SerdeTests.testISerializableRoundTrip(
        new HashPartitionFunctionSpec(inputType, ImmutableList.of(0)));
  }

  @Test
  public void testRoundRobinPartitionFunctionSpec() {
    SerdeTests.testISerializableRoundTrip(new RoundRobinPartitionFunctionSpec());
  }
}

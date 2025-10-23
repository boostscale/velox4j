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
package org.boostscale.velox4j.data;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.*;

import org.boostscale.velox4j.Velox4j;
import org.boostscale.velox4j.memory.BytesAllocationListener;
import org.boostscale.velox4j.memory.MemoryManager;
import org.boostscale.velox4j.session.Session;
import org.boostscale.velox4j.test.ResourceTests;
import org.boostscale.velox4j.test.Velox4jTests;

public class RowVectorTest {
  private static BytesAllocationListener allocationListener;
  private static MemoryManager memoryManager;
  private static Session session;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
    allocationListener = new BytesAllocationListener();
    memoryManager = Velox4j.newMemoryManager(allocationListener);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    memoryManager.close();
    Assert.assertEquals(0, allocationListener.currentBytes());
  }

  @Before
  public void setUp() throws Exception {
    session = Velox4j.newSession(memoryManager);
  }

  @After
  public void tearDown() throws Exception {
    session.close();
  }

  @Test
  public void testPartitionByKeysSinglePartitionKey() {
    final RowVector input = BaseVectorTests.newSampleRowVector(session);
    Assert.assertEquals(3, input.getSize());
    final List<RowVector> out0 = session.rowVectorOps().partitionByKeys(input, ImmutableList.of(0));
    Assert.assertEquals(3, out0.size());
    Assert.assertEquals(
        ResourceTests.readResourceAsString("vector-output/partition-by-keys-1.txt"),
        BaseVectors.toString(out0));

    final List<RowVector> out1 = session.rowVectorOps().partitionByKeys(input, ImmutableList.of(1));
    Assert.assertEquals(1, out1.size());
    Assert.assertEquals(
        ResourceTests.readResourceAsString("vector-output/partition-by-keys-2.txt"),
        BaseVectors.toString(out1));
  }

  @Test
  public void testPartitionByKeysMultiplePartitionKeys() {
    final RowVector input = BaseVectorTests.newSampleRowVector(session);
    Assert.assertEquals(3, input.getSize());
    final List<RowVector> out0 =
        session.rowVectorOps().partitionByKeys(input, ImmutableList.of(1, 2));
    Assert.assertEquals(3, out0.size());
    Assert.assertEquals(
        ResourceTests.readResourceAsString("vector-output/partition-by-keys-3.txt"),
        BaseVectors.toString(out0));
  }
}

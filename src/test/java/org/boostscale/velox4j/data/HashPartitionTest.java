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
import org.boostscale.velox4j.test.Velox4jTests;

public class HashPartitionTest {
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
  public void testHashPartitionConsistency() {
    // Sample RowVector has 3 rows with BIGINT columns (c0, a1)
    final RowVector input = BaseVectorTests.newSampleRowVector(session);
    Assert.assertEquals(3, input.getSize());

    final int numPartitions = 4;
    final List<RowVector> partitions =
        session.rowVectorOps().hashPartition(input, ImmutableList.of(0), numPartitions);
    Assert.assertEquals(numPartitions, partitions.size());

    int totalRows = 0;
    for (RowVector p : partitions) {
      if (p != null) {
        totalRows += p.getSize();
      }
    }
    Assert.assertEquals(3, totalRows);
  }

  @Test
  public void testHashPartitionDeterministic() {
    final RowVector input = BaseVectorTests.newSampleRowVector(session);
    final int numPartitions = 4;

    final List<RowVector> run1 =
        session.rowVectorOps().hashPartition(input, ImmutableList.of(0), numPartitions);
    final List<RowVector> run2 =
        session.rowVectorOps().hashPartition(input, ImmutableList.of(0), numPartitions);

    for (int i = 0; i < numPartitions; i++) {
      if (run1.get(i) == null) {
        Assert.assertNull(run2.get(i));
      } else {
        Assert.assertNotNull(run2.get(i));
        Assert.assertEquals(run1.get(i).getSize(), run2.get(i).getSize());
        Assert.assertEquals(run1.get(i).toString(), run2.get(i).toString());
      }
    }
  }

  @Test
  public void testHashPartitionAndSerializeRoundTrip() {
    final RowVector input = BaseVectorTests.newSampleRowVector(session);
    final int numPartitions = 4;

    final byte[][] serialized =
        session.rowVectorOps().hashPartitionAndSerialize(input, ImmutableList.of(0), numPartitions);
    Assert.assertEquals(numPartitions, serialized.length);

    int totalRows = 0;
    for (byte[] buf : serialized) {
      if (buf != null) {
        final BaseVector deserialized = session.baseVectorOps().deserializeOneFromBuf(buf);
        Assert.assertNotNull(deserialized);
        totalRows += deserialized.getSize();
      }
    }
    Assert.assertEquals(3, totalRows);
  }

  @Test
  public void testSerializeOneToBufRoundTrip() {
    final RowVector input = BaseVectorTests.newSampleRowVector(session);
    final byte[] buf = BaseVectors.serializeOneToBuf(input);
    Assert.assertNotNull(buf);
    Assert.assertTrue(buf.length > 0);

    final BaseVector deserialized = session.baseVectorOps().deserializeOneFromBuf(buf);
    Assert.assertEquals(input.getSize(), deserialized.getSize());
    Assert.assertEquals(input.toString(), deserialized.toString());
  }

  @Test
  public void testHashPartitionEmptyPartitions() {
    final RowVector input = BaseVectorTests.newSampleRowVector(session);
    // With 64 partitions and only 3 rows, most partitions should be empty
    final int numPartitions = 64;

    final List<RowVector> partitions =
        session.rowVectorOps().hashPartition(input, ImmutableList.of(0), numPartitions);
    Assert.assertEquals(numPartitions, partitions.size());

    int nonNull = 0;
    int totalRows = 0;
    for (RowVector p : partitions) {
      if (p != null) {
        nonNull++;
        totalRows += p.getSize();
      }
    }
    Assert.assertTrue("Expected sparse output", nonNull < numPartitions);
    Assert.assertEquals(3, totalRows);
  }
}

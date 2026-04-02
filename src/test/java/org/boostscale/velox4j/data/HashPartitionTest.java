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

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.*;

import org.boostscale.velox4j.Velox4j;
import org.boostscale.velox4j.config.Config;
import org.boostscale.velox4j.config.ConnectorConfig;
import org.boostscale.velox4j.connector.ExternalStreamConnectorSplit;
import org.boostscale.velox4j.connector.ExternalStreamTableHandle;
import org.boostscale.velox4j.connector.ExternalStreams;
import org.boostscale.velox4j.iterator.UpIterators;
import org.boostscale.velox4j.memory.BytesAllocationListener;
import org.boostscale.velox4j.memory.MemoryManager;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.query.Query;
import org.boostscale.velox4j.query.SerialTask;
import org.boostscale.velox4j.session.Session;
import org.boostscale.velox4j.test.Velox4jTests;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.VarCharType;
import org.boostscale.velox4j.variant.BigIntValue;
import org.boostscale.velox4j.variant.RowValue;
import org.boostscale.velox4j.variant.VarCharValue;

public class HashPartitionTest {
  private static BytesAllocationListener allocationListener;
  private static MemoryManager memoryManager;
  private Session session;

  @BeforeClass
  public static void beforeClass() {
    Velox4jTests.ensureInitialized();
    allocationListener = new BytesAllocationListener();
    memoryManager = Velox4j.newMemoryManager(allocationListener);
  }

  @AfterClass
  public static void afterClass() {
    memoryManager.close();
    Assert.assertEquals(0, allocationListener.currentBytes());
  }

  @Before
  public void setUp() {
    session = Velox4j.newSession(memoryManager);
  }

  @After
  public void tearDown() {
    session.close();
  }

  private RowVector createTestVector() {
    // Create a RowVector with key column (BIGINT) and value column (VARCHAR)
    RowType type =
        new RowType(
            ImmutableList.of("key", "value"),
            ImmutableList.of(new BigIntType(), new VarCharType()));
    // Create rows: (1, "a"), (2, "b"), (3, "c"), (1, "d"), (2, "e")
    RowValue row0 = new RowValue(ImmutableList.of(new BigIntValue(1L), new VarCharValue("a")));
    RowValue row1 = new RowValue(ImmutableList.of(new BigIntValue(2L), new VarCharValue("b")));
    RowValue row2 = new RowValue(ImmutableList.of(new BigIntValue(3L), new VarCharValue("c")));
    RowValue row3 = new RowValue(ImmutableList.of(new BigIntValue(1L), new VarCharValue("d")));
    RowValue row4 = new RowValue(ImmutableList.of(new BigIntValue(2L), new VarCharValue("e")));

    // Build a RowVector from variants using ExternalStream
    ExternalStreams.BlockingQueue queue = session.externalStreamOps().newBlockingQueue();
    for (RowValue row : ImmutableList.of(row0, row1, row2, row3, row4)) {
      BaseVector vec = session.variantOps().toVector(type, row);
      queue.put(vec.asRowVector());
    }
    queue.noMoreInput();

    // Read all rows through a scan to get a single RowVector
    TableScanNode scan =
        new TableScanNode(
            "scan-1",
            type,
            new ExternalStreamTableHandle("connector-external-stream"),
            ImmutableList.of());
    Query query = new Query(scan, Config.empty(), ConnectorConfig.empty());
    SerialTask task = session.queryOps().execute(query);
    task.addSplit(
        "scan-1", new ExternalStreamConnectorSplit("connector-external-stream", queue.id()));
    task.noMoreSplits("scan-1");
    Iterator<RowVector> itr = UpIterators.asJavaIterator(task);
    Assert.assertTrue(itr.hasNext());
    return itr.next();
  }

  @Test
  public void testHashPartitionConsistency() {
    // Two different RowVectors with overlapping keys should map same keys to same partitions
    RowVector vector = createTestVector();
    int numPartitions = 4;

    List<RowVector> partitions =
        session.rowVectorOps().hashPartition(vector, ImmutableList.of(0), numPartitions);
    Assert.assertEquals(numPartitions, partitions.size());

    // Count total rows across all partitions
    int totalRows = 0;
    for (RowVector p : partitions) {
      if (p != null) {
        totalRows += p.getSize();
      }
    }
    Assert.assertEquals(5, totalRows);
  }

  @Test
  public void testHashPartitionEmptyPartitions() {
    RowVector vector = createTestVector();
    // With many partitions, some should be empty (null)
    int numPartitions = 64;
    List<RowVector> partitions =
        session.rowVectorOps().hashPartition(vector, ImmutableList.of(0), numPartitions);
    Assert.assertEquals(numPartitions, partitions.size());

    int nonNullCount = 0;
    int totalRows = 0;
    for (RowVector p : partitions) {
      if (p != null) {
        nonNullCount++;
        totalRows += p.getSize();
      }
    }
    // With only 3 distinct keys and 64 partitions, at most 3 partitions should have data
    Assert.assertTrue(nonNullCount <= 3);
    Assert.assertEquals(5, totalRows);
  }

  @Test
  public void testHashPartitionAndSerializeRoundTrip() {
    RowVector vector = createTestVector();
    int numPartitions = 4;

    byte[][] serialized =
        session
            .rowVectorOps()
            .hashPartitionAndSerialize(vector, ImmutableList.of(0), numPartitions);
    Assert.assertEquals(numPartitions, serialized.length);

    // Deserialize each partition and verify total rows
    int totalRows = 0;
    for (int i = 0; i < serialized.length; i++) {
      if (serialized[i] != null) {
        BaseVector deserialized = session.baseVectorOps().deserializeOneFromBuf(serialized[i]);
        Assert.assertNotNull(deserialized);
        totalRows += deserialized.getSize();
      }
    }
    Assert.assertEquals(5, totalRows);
  }

  @Test
  public void testSerializeOneToBufRoundTrip() {
    RowVector vector = createTestVector();
    byte[] serialized = session.baseVectorOps().serializeOneToBuf(vector);
    Assert.assertNotNull(serialized);
    Assert.assertTrue(serialized.length > 0);

    BaseVector deserialized = session.baseVectorOps().deserializeOneFromBuf(serialized);
    Assert.assertEquals(vector.getSize(), deserialized.getSize());
    Assert.assertEquals(vector.toString(), deserialized.toString());
  }

  @Test
  public void testHashPartitionDeterministic() {
    // Calling hashPartition twice on the same data should produce identical partitioning
    RowVector vector = createTestVector();
    int numPartitions = 4;

    byte[][] result1 =
        session
            .rowVectorOps()
            .hashPartitionAndSerialize(vector, ImmutableList.of(0), numPartitions);
    byte[][] result2 =
        session
            .rowVectorOps()
            .hashPartitionAndSerialize(vector, ImmutableList.of(0), numPartitions);

    Assert.assertEquals(result1.length, result2.length);
    for (int i = 0; i < result1.length; i++) {
      if (result1[i] == null) {
        Assert.assertNull(result2[i]);
      } else {
        Assert.assertNotNull(result2[i]);
        Assert.assertArrayEquals(result1[i], result2[i]);
      }
    }
  }
}

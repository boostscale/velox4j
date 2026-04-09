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
package org.boostscale.velox4j.query;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.*;

import org.boostscale.velox4j.Velox4j;
import org.boostscale.velox4j.config.Config;
import org.boostscale.velox4j.config.ConnectorConfig;
import org.boostscale.velox4j.connector.*;
import org.boostscale.velox4j.data.BaseVector;
import org.boostscale.velox4j.data.BaseVectors;
import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.iterator.UpIterators;
import org.boostscale.velox4j.join.JoinType;
import org.boostscale.velox4j.memory.BytesAllocationListener;
import org.boostscale.velox4j.memory.MemoryManager;
import org.boostscale.velox4j.plan.HashJoinNode;
import org.boostscale.velox4j.plan.TableScanNode;
import org.boostscale.velox4j.session.Session;
import org.boostscale.velox4j.test.Velox4jTests;
import org.boostscale.velox4j.test.dataset.TestDataFile;
import org.boostscale.velox4j.test.dataset.tpch.TpchDatasets;
import org.boostscale.velox4j.test.dataset.tpch.TpchTableName;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.VarCharType;

/**
 * Simulates a distributed shuffle join using velox4j primitives.
 *
 * <p>The flow mirrors what an MPP engine would do:
 *
 * <ol>
 *   <li>Scan left and right tables (TPCH nation and region)
 *   <li>Hash-partition both sides by join key, then serialize each partition
 *   <li>Simulate network transfer (in-process: byte[] round-trip)
 *   <li>Deserialize and feed each partition into BlockingQueues
 *   <li>Execute HashJoinNode with ExternalStream sources
 *   <li>Verify join output correctness
 * </ol>
 */
public class ShuffleJoinTest {
  private static final String HIVE_CONNECTOR_ID = "connector-hive";
  private static final String ES_CONNECTOR_ID = "connector-external-stream";
  private static TestDataFile NATION_FILE;
  private static TestDataFile REGION_FILE;
  private static BytesAllocationListener allocationListener;
  private static MemoryManager memoryManager;
  private Session session;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
    allocationListener = new BytesAllocationListener();
    memoryManager = Velox4j.newMemoryManager(allocationListener);
    NATION_FILE = TpchDatasets.get().get(TpchTableName.NATION);
    REGION_FILE = TpchDatasets.get().get(TpchTableName.REGION);
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

  /**
   * Simulates a shuffle join between NATION and REGION tables. Both sides are hash-partitioned by
   * the join key (regionkey), serialized, "transferred" (in-process byte[] round-trip), then
   * deserialized and fed into a local HashJoinNode via ExternalStream.
   */
  @Test
  public void testShuffleJoin() {
    // Step 1: Scan both tables from Parquet files
    List<RowVector> nationBatches = scanTable(NATION_FILE);
    List<RowVector> regionBatches = scanTable(REGION_FILE);
    Assert.assertFalse("Nation table should have data", nationBatches.isEmpty());
    Assert.assertFalse("Region table should have data", regionBatches.isEmpty());

    // Step 2: Hash-partition both sides by join key
    // Nation: column 2 = n_regionkey; Region: column 0 = r_regionkey
    int numPartitions = 4;
    List<List<byte[]>> nationPartitions = partitionAndSerialize(nationBatches, 2, numPartitions);
    List<List<byte[]>> regionPartitions = partitionAndSerialize(regionBatches, 0, numPartitions);

    // Step 3: For each partition, deserialize both sides and execute local join
    int totalJoinedRows = 0;
    for (int pid = 0; pid < numPartitions; pid++) {
      List<byte[]> leftBufs = nationPartitions.get(pid);
      List<byte[]> rightBufs = regionPartitions.get(pid);
      if (leftBufs.isEmpty() && rightBufs.isEmpty()) {
        continue;
      }

      // Deserialize and feed into BlockingQueues
      ExternalStreams.BlockingQueue leftQueue = session.externalStreamOps().newBlockingQueue();
      ExternalStreams.BlockingQueue rightQueue = session.externalStreamOps().newBlockingQueue();

      for (byte[] buf : leftBufs) {
        BaseVector vec = session.baseVectorOps().deserializeOneFromBuf(buf);
        leftQueue.put(vec.asRowVector());
      }
      leftQueue.noMoreInput();

      for (byte[] buf : rightBufs) {
        BaseVector vec = session.baseVectorOps().deserializeOneFromBuf(buf);
        rightQueue.put(vec.asRowVector());
      }
      rightQueue.noMoreInput();

      // Build join plan with ExternalStream sources
      RowType nationType = NATION_FILE.schema();
      RowType regionType = REGION_FILE.schema();
      RowType joinOutputType =
          new RowType(
              ImmutableList.of("n_nationkey", "n_name", "r_regionkey", "r_name"),
              ImmutableList.of(
                  new BigIntType(), new VarCharType(), new BigIntType(), new VarCharType()));

      TableScanNode leftScan =
          new TableScanNode(
              "left-scan",
              nationType,
              new ExternalStreamTableHandle(ES_CONNECTOR_ID),
              ImmutableList.of());
      TableScanNode rightScan =
          new TableScanNode(
              "right-scan",
              regionType,
              new ExternalStreamTableHandle(ES_CONNECTOR_ID),
              ImmutableList.of());

      HashJoinNode join =
          new HashJoinNode(
              "join-1",
              JoinType.INNER,
              ImmutableList.of(FieldAccessTypedExpr.create(new BigIntType(), "n_regionkey")),
              ImmutableList.of(FieldAccessTypedExpr.create(new BigIntType(), "r_regionkey")),
              null,
              leftScan,
              rightScan,
              joinOutputType,
              false,
              false);

      Query query = new Query(join, Config.empty(), ConnectorConfig.empty());
      SerialTask task = session.queryOps().execute(query);
      task.addSplit("left-scan", new ExternalStreamConnectorSplit(ES_CONNECTOR_ID, leftQueue.id()));
      task.addSplit(
          "right-scan", new ExternalStreamConnectorSplit(ES_CONNECTOR_ID, rightQueue.id()));
      task.noMoreSplits("left-scan");
      task.noMoreSplits("right-scan");

      Iterator<RowVector> results = UpIterators.asJavaIterator(task);
      while (results.hasNext()) {
        RowVector batch = results.next();
        totalJoinedRows += batch.getSize();
      }
    }

    // TPCH NATION has 25 rows, REGION has 5 rows.
    // Each nation has exactly one region, so INNER JOIN produces 25 rows.
    Assert.assertEquals(25, totalJoinedRows);
  }

  /**
   * Simulates a broadcast join: the small table (REGION) is broadcast to all partitions, while the
   * large table (NATION) is scanned locally per partition.
   */
  @Test
  public void testBroadcastJoin() {
    // Step 1: Scan the small table and serialize it (this would be broadcast to all nodes).
    // Flatten to materialize lazy columns from Parquet scan before serializing.
    List<RowVector> regionBatches = scanTable(REGION_FILE);
    List<byte[]> broadcastBuffers = new ArrayList<>();
    for (RowVector batch : regionBatches) {
      broadcastBuffers.add(BaseVectors.serializeOneToBuf(batch.flattenedVector()));
    }

    // Step 2: Scan the large table (would be local shards on each data node).
    // Flatten to materialize lazy columns before feeding into the join.
    List<RowVector> nationBatches = scanTable(NATION_FILE);

    // Step 3: On each "data node", join local nation data with broadcast region data
    ExternalStreams.BlockingQueue leftQueue = session.externalStreamOps().newBlockingQueue();
    ExternalStreams.BlockingQueue rightQueue = session.externalStreamOps().newBlockingQueue();

    // Feed nation data (probe side)
    for (RowVector batch : nationBatches) {
      leftQueue.put(batch.flattenedVector().asRowVector());
    }
    leftQueue.noMoreInput();

    // Deserialize and feed broadcast region data (build side)
    for (byte[] buf : broadcastBuffers) {
      BaseVector vec = session.baseVectorOps().deserializeOneFromBuf(buf);
      rightQueue.put(vec.asRowVector());
    }
    rightQueue.noMoreInput();

    // Build join plan
    RowType nationType = NATION_FILE.schema();
    RowType regionType = REGION_FILE.schema();
    RowType joinOutputType =
        new RowType(
            ImmutableList.of("n_nationkey", "n_name", "r_regionkey", "r_name"),
            ImmutableList.of(
                new BigIntType(), new VarCharType(), new BigIntType(), new VarCharType()));

    TableScanNode leftScan =
        new TableScanNode(
            "left-scan",
            nationType,
            new ExternalStreamTableHandle(ES_CONNECTOR_ID),
            ImmutableList.of());
    TableScanNode rightScan =
        new TableScanNode(
            "right-scan",
            regionType,
            new ExternalStreamTableHandle(ES_CONNECTOR_ID),
            ImmutableList.of());

    HashJoinNode join =
        new HashJoinNode(
            "join-1",
            JoinType.LEFT,
            ImmutableList.of(FieldAccessTypedExpr.create(new BigIntType(), "n_regionkey")),
            ImmutableList.of(FieldAccessTypedExpr.create(new BigIntType(), "r_regionkey")),
            null,
            leftScan,
            rightScan,
            joinOutputType,
            false,
            false);

    Query query = new Query(join, Config.empty(), ConnectorConfig.empty());
    SerialTask task = session.queryOps().execute(query);
    task.addSplit("left-scan", new ExternalStreamConnectorSplit(ES_CONNECTOR_ID, leftQueue.id()));
    task.addSplit("right-scan", new ExternalStreamConnectorSplit(ES_CONNECTOR_ID, rightQueue.id()));
    task.noMoreSplits("left-scan");
    task.noMoreSplits("right-scan");

    int totalRows = 0;
    Iterator<RowVector> results = UpIterators.asJavaIterator(task);
    while (results.hasNext()) {
      totalRows += results.next().getSize();
    }
    Assert.assertEquals(25, totalRows);
  }

  private List<RowVector> scanTable(TestDataFile dataFile) {
    File file = dataFile.file();
    RowType schema = dataFile.schema();
    TableScanNode scan = newHiveScanNode("scan-" + file.getName(), schema);
    ConnectorSplit split = newHiveSplit(file);
    Query query = new Query(scan, Config.empty(), ConnectorConfig.empty());
    SerialTask task = session.queryOps().execute(query);
    task.addSplit(scan.getId(), split);
    task.noMoreSplits(scan.getId());

    List<RowVector> batches = new ArrayList<>();
    Iterator<RowVector> itr = UpIterators.asJavaIterator(task);
    while (itr.hasNext()) {
      batches.add(itr.next());
    }
    return batches;
  }

  /**
   * Hash-partition multiple batches by key column and serialize. Returns a list of numPartitions
   * lists, where each inner list contains serialized buffers for that partition.
   */
  private List<List<byte[]>> partitionAndSerialize(
      List<RowVector> batches, int keyChannel, int numPartitions) {
    List<List<byte[]>> result = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      result.add(new ArrayList<>());
    }
    for (RowVector batch : batches) {
      List<RowVector> partitions =
          session
              .rowVectorOps()
              .partitionByKeyHashes(batch, ImmutableList.of(keyChannel), numPartitions);
      for (int pid = 0; pid < partitions.size(); pid++) {
        if (partitions.get(pid) != null) {
          result.get(pid).add(BaseVectors.serializeOneToBuf(partitions.get(pid)));
        }
      }
    }
    return result;
  }

  private static TableScanNode newHiveScanNode(String id, RowType schema) {
    return new TableScanNode(
        id,
        schema,
        new HiveTableHandle(
            HIVE_CONNECTOR_ID, "table-1", ImmutableList.of(), null, schema, ImmutableMap.of()),
        toAssignments(schema));
  }

  private static ConnectorSplit newHiveSplit(File file) {
    return new HiveConnectorSplit(
        HIVE_CONNECTOR_ID,
        0,
        false,
        file.getAbsolutePath(),
        FileFormat.PARQUET,
        0,
        file.length(),
        ImmutableMap.of(),
        null,
        null,
        ImmutableMap.of(),
        null,
        ImmutableMap.of(),
        ImmutableMap.of(),
        null,
        null);
  }

  private static List<Assignment> toAssignments(RowType rowType) {
    List<Assignment> assignments = new ArrayList<>();
    for (int i = 0; i < rowType.size(); i++) {
      String name = rowType.getNames().get(i);
      assignments.add(
          new Assignment(
              name,
              new HiveColumnHandle(
                  name,
                  ColumnType.REGULAR,
                  rowType.getChildren().get(i),
                  rowType.getChildren().get(i),
                  ImmutableList.of())));
    }
    return assignments;
  }
}

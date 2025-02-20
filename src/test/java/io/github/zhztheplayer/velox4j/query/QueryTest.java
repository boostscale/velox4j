package io.github.zhztheplayer.velox4j.query;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.aggregate.AggregateStep;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.connector.Assignment;
import io.github.zhztheplayer.velox4j.connector.ColumnType;
import io.github.zhztheplayer.velox4j.connector.ExternalStream;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.connector.FileFormat;
import io.github.zhztheplayer.velox4j.connector.HiveColumnHandle;
import io.github.zhztheplayer.velox4j.connector.HiveConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.HiveTableHandle;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.join.JoinType;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.AggregationNode;
import io.github.zhztheplayer.velox4j.plan.FilterNode;
import io.github.zhztheplayer.velox4j.plan.HashJoinNode;
import io.github.zhztheplayer.velox4j.plan.LimitNode;
import io.github.zhztheplayer.velox4j.plan.OrderByNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.sort.SortOrder;
import io.github.zhztheplayer.velox4j.test.ResourceTests;
import io.github.zhztheplayer.velox4j.test.SampleQueryTests;
import io.github.zhztheplayer.velox4j.test.TpchTests;
import io.github.zhztheplayer.velox4j.test.UpIteratorTests;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.variant.BigIntValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

public class QueryTest {
  private static MemoryManager memoryManager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
    memoryManager = MemoryManager.create(AllocationListener.NOOP);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    memoryManager.close();
  }

  @Test
  public void testHiveScan1() {
    final Session session = Velox4j.newSession(memoryManager);
    final File file = TpchTests.Table.NATION.file();
    final RowType outputType = TpchTests.Table.NATION.schema();
    final TableScanNode scanNode = newSampleScanNode("id-1", outputType);
    final List<BoundSplit> splits = List.of(
        newSampleSplit(scanNode, file)
    );
    final Query query = new Query(scanNode, splits, Config.empty(), ConnectorConfig.empty());
    final UpIterator itr = session.queryOps().execute(query);
    UpIteratorTests.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0, ResourceTests.readResourceAsString("query-output/tpch-scan-nation.tsv"))
        .run();
    session.close();
  }


  @Test
  public void testHiveScan2() {
    final Session session = Velox4j.newSession(memoryManager);
    final File file = TpchTests.Table.REGION.file();
    final RowType outputType = TpchTests.Table.REGION.schema();
    final TableScanNode scanNode = newSampleScanNode("id-1", outputType);
    final List<BoundSplit> splits = List.of(
        newSampleSplit(scanNode, file)
    );
    final Query query = new Query(scanNode, splits, Config.empty(), ConnectorConfig.empty());
    final UpIterator itr = session.queryOps().execute(query);
    UpIteratorTests.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0, ResourceTests.readResourceAsString("query-output/tpch-scan-region.tsv"))
        .run();
    session.close();
  }

  @Test
  public void testAggregate() {
    final Session session = Velox4j.newSession(memoryManager);
    final File file = TpchTests.Table.NATION.file();
    final RowType outputType = TpchTests.Table.NATION.schema();
    final TableScanNode scanNode = newSampleScanNode("id-1", outputType);
    final List<BoundSplit> splits = List.of(
        newSampleSplit(scanNode, file)
    );
    final AggregationNode aggregationNode = new AggregationNode("id-2", AggregateStep.SINGLE,
        List.of(FieldAccessTypedExpr.create(new BigIntType(), "n_regionkey")),
        List.of(),
        List.of("cnt"),
        List.of(new Aggregate(
            new CallTypedExpr(new BigIntType(), List.of(
                FieldAccessTypedExpr.create(new BigIntType(), "n_nationkey")),
                "sum"),
            List.of(new BigIntType()),
            null,
            List.of(),
            List.of(),
            false
        )),
        false,
        List.of(scanNode),
        null,
        List.of()
    );
    final Query query = new Query(aggregationNode, splits, Config.empty(), ConnectorConfig.empty());
    final UpIterator itr = session.queryOps().execute(query);
    UpIteratorTests.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0, ResourceTests.readResourceAsString("query-output/tpch-aggregate-1.tsv"))
        .run();
    session.close();
  }

  @Test
  public void testExternalStream() {
    final Session session = Velox4j.newSession(memoryManager);
    final String json = SampleQueryTests.readQueryJson();
    final UpIterator sampleIn = session.queryOps().execute(Serde.fromJson(json, Query.class));
    final DownIterator down = new DownIterator(sampleIn);
    final ExternalStream es = session.externalStreamOps().bind(down);
    final TableScanNode scanNode = new TableScanNode(
        "id-1",
        SampleQueryTests.getSchema(),
        new ExternalStreamTableHandle("connector-external-stream"),
        List.of()
    );
    final List<BoundSplit> splits = List.of(
        new BoundSplit(
            "id-1",
            -1,
            new ExternalStreamConnectorSplit("connector-external-stream", es.id())
        )
    );
    final Query query = new Query(scanNode, splits, Config.empty(), ConnectorConfig.empty());
    final UpIterator out = session.queryOps().execute(query);
    SampleQueryTests.assertIterator(out);
    session.close();
  }

  @Test
  public void testProject() {
    final Session session = Velox4j.newSession(memoryManager);
    final File file = TpchTests.Table.NATION.file();
    final RowType outputType = TpchTests.Table.NATION.schema();
    final TableScanNode scanNode = newSampleScanNode("id-1", outputType);
    final List<BoundSplit> splits = List.of(
        newSampleSplit(scanNode, file)
    );
    final ProjectNode projectNode = new ProjectNode("id-2", List.of(scanNode),
        List.of("n_nationkey", "n_comment"),
        List.of(
            FieldAccessTypedExpr.create(new BigIntType(), "n_nationkey"),
            FieldAccessTypedExpr.create(new VarCharType(), "n_comment")
        ));
    final Query query = new Query(projectNode, splits, Config.empty(), ConnectorConfig.empty());
    final UpIterator itr = session.queryOps().execute(query);
    UpIteratorTests.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0, ResourceTests.readResourceAsString("query-output/tpch-project-1.tsv"))
        .run();
    session.close();
  }

  @Test
  public void testFilter() {
    final Session session = Velox4j.newSession(memoryManager);
    final File file = TpchTests.Table.NATION.file();
    final RowType outputType = TpchTests.Table.NATION.schema();
    final TableScanNode scanNode = newSampleScanNode("id-1", outputType);
    final List<BoundSplit> splits = List.of(
        newSampleSplit(scanNode, file)
    );
    final FilterNode filterNode = new FilterNode("id-2", List.of(scanNode),
        new CallTypedExpr(new BooleanType(), List.of(
            FieldAccessTypedExpr.create(new BigIntType(), "n_regionkey"),
            ConstantTypedExpr.create(new BigIntValue(3))),
            "greaterthanorequal"));
    final Query query = new Query(filterNode, splits, Config.empty(), ConnectorConfig.empty());
    final UpIterator itr = session.queryOps().execute(query);
    UpIteratorTests.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0, ResourceTests.readResourceAsString("query-output/tpch-filter-1.tsv"))
        .run();
    session.close();
  }

  @Test
  public void testHashJoin() {
    final Session session = Velox4j.newSession(memoryManager);
    final File nationFile = TpchTests.Table.NATION.file();
    final RowType nationOutputType = TpchTests.Table.NATION.schema();
    final File regionFile = TpchTests.Table.REGION.file();
    final RowType regionOutputType = TpchTests.Table.REGION.schema();
    final TableScanNode nationScanNode = newSampleScanNode("id-1", nationOutputType);
    final TableScanNode regionScanNode = newSampleScanNode("id-2", regionOutputType);
    final List<BoundSplit> splits = List.of(
        newSampleSplit(nationScanNode, nationFile),
        newSampleSplit(regionScanNode, regionFile)
    );
    final HashJoinNode hashJoinNode = new HashJoinNode("id-3",
        JoinType.LEFT,
        List.of(FieldAccessTypedExpr.create(new BigIntType(), "n_regionkey")),
        List.of(FieldAccessTypedExpr.create(new BigIntType(), "r_regionkey")),
        null,
        nationScanNode,
        regionScanNode,
        new RowType(List.of("n_nationkey", "n_name", "r_regionkey", "r_name"),
            List.of(new BigIntType(), new VarCharType(), new BigIntType(), new VarCharType())),
        false
    );
    final Query query = new Query(hashJoinNode, splits, Config.empty(), ConnectorConfig.empty());
    final UpIterator itr = session.queryOps().execute(query);
    UpIteratorTests.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0, ResourceTests.readResourceAsString("query-output/tpch-join-1.tsv"))
        .run();
    session.close();
  }

  @Test
  public void testOrderBy() {
    final Session session = Velox4j.newSession(memoryManager);
    final File file = TpchTests.Table.NATION.file();
    final RowType outputType = TpchTests.Table.NATION.schema();
    final TableScanNode scanNode = newSampleScanNode("id-1", outputType);
    final List<BoundSplit> splits = List.of(
        newSampleSplit(scanNode, file)
    );
    final OrderByNode orderByNode = new OrderByNode("id-2", List.of(scanNode),
        List.of(FieldAccessTypedExpr.create(new BigIntType(), "n_regionkey"),
            FieldAccessTypedExpr.create(new BigIntType(), "n_nationkey")),
        List.of(new SortOrder(true, false),
            new SortOrder(false, false)),
        false);
    final Query query = new Query(orderByNode, splits, Config.empty(), ConnectorConfig.empty());
    final UpIterator itr = session.queryOps().execute(query);
    UpIteratorTests.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0, ResourceTests.readResourceAsString("query-output/tpch-orderby-1.tsv"))
        .run();
    session.close();
  }

  @Test
  public void testLimit() {
    final Session session = Velox4j.newSession(memoryManager);
    final File file = TpchTests.Table.NATION.file();
    final RowType outputType = TpchTests.Table.NATION.schema();
    final TableScanNode scanNode = newSampleScanNode("id-1", outputType);
    final List<BoundSplit> splits = List.of(
        newSampleSplit(scanNode, file)
    );
    final LimitNode limitNode = new LimitNode("id-2", List.of(scanNode), 5, 3, false);
    final Query query = new Query(limitNode, splits, Config.empty(), ConnectorConfig.empty());
    final UpIterator itr = session.queryOps().execute(query);
    UpIteratorTests.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0, ResourceTests.readResourceAsString("query-output/tpch-limit-1.tsv"))
        .run();
    session.close();
  }

  private static List<Assignment> toAssignments(RowType rowType) {
    final List<Assignment> list = new ArrayList<>();
    for (int i = 0; i < rowType.size(); i++) {
      final String name = rowType.getNames().get(i);
      final Type type = rowType.getChildren().get(i);
      list.add(new Assignment(name,
          new HiveColumnHandle(name, ColumnType.REGULAR, type, type, List.of())));
    }
    return list;
  }

  private static BoundSplit newSampleSplit(TableScanNode scanNode, File file) {
    return new BoundSplit(
        scanNode.getId(),
        -1,
        new HiveConnectorSplit(
            "connector-hive",
            0,
            false,
            file.getAbsolutePath(),
            FileFormat.PARQUET,
            0,
            file.length(),
            Map.of(),
            OptionalInt.empty(),
            Optional.empty(),
            Map.of(),
            Optional.empty(),
            Map.of(),
            Map.of(),
            Optional.empty(),
            Optional.empty()
        )
    );
  }

  private static TableScanNode newSampleScanNode(String planNodeId, RowType outputType) {
    final TableScanNode scanNode = new TableScanNode(
        planNodeId,
        outputType,
        new HiveTableHandle(
            "connector-hive",
            "tab-1",
            false,
            List.of(),
            null,
            outputType,
            Map.of()
        ),
        toAssignments(outputType)
    );
    return scanNode;
  }
}

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
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.AggregationNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.test.ResourceTests;
import io.github.zhztheplayer.velox4j.test.SampleQueryTests;
import io.github.zhztheplayer.velox4j.test.TpchTests;
import io.github.zhztheplayer.velox4j.test.UpIteratorTests;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
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
    Velox4j.ensureInitialized();
    memoryManager = MemoryManager.create(AllocationListener.NOOP);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    memoryManager.close();
  }

  @Test
  public void testHiveScan() {
    final JniApi jniApi = JniApi.create(memoryManager);
    final File file = TpchTests.Table.NATION.file();
    final RowType outputType = TpchTests.Table.NATION.schema();
    final TableScanNode scanNode = newSampleScanNode(outputType);
    final List<BoundSplit> splits = List.of(
        newSampleSplit(scanNode, file)
    );
    final Query query = new Query(scanNode, splits, Config.empty(), ConnectorConfig.empty());
    final UpIterator itr = query.execute(jniApi);
    UpIteratorTests.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0, ResourceTests.readResourceAsString("query-output/tpch-nation.tsv"))
        .run();
    jniApi.close();
  }

  @Test
  public void testAggregate() {
    final JniApi jniApi = JniApi.create(memoryManager);
    final File file = TpchTests.Table.NATION.file();
    final RowType outputType = TpchTests.Table.NATION.schema();
    final TableScanNode scanNode = newSampleScanNode(outputType);
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
    final UpIterator itr = query.execute(jniApi);
    UpIteratorTests.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0, ResourceTests.readResourceAsString("query-output/tpch-nation-aggregate-1.tsv"))
        .run();
    jniApi.close();
  }

  @Test
  public void testExternalStream() {
    final JniApi jniApi = JniApi.create(memoryManager);
    final String json = SampleQueryTests.readQueryJson();
    final UpIterator sampleIn = jniApi.executeQuery(json);
    final DownIterator down = new DownIterator(sampleIn);
    final ExternalStream es = jniApi.newExternalStream(down);
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
    final UpIterator out = query.execute(jniApi);
    SampleQueryTests.assertIterator(out);
    jniApi.close();
  }

  @Test
  public void testProject() {
    final JniApi jniApi = JniApi.create(memoryManager);
    final File file = TpchTests.Table.NATION.file();
    final RowType outputType = TpchTests.Table.NATION.schema();
    final TableScanNode scanNode = newSampleScanNode(outputType);
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
    final UpIterator itr = query.execute(jniApi);
    UpIteratorTests.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0, ResourceTests.readResourceAsString("query-output/tpch-nation-project-1.tsv"))
        .run();
    jniApi.close();
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

  private static TableScanNode newSampleScanNode(RowType outputType) {
    final TableScanNode scanNode = new TableScanNode(
        "id-1",
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

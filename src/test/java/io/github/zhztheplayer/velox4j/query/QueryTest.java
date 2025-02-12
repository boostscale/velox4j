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
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.AggregationNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.test.UpIteratorTests;
import io.github.zhztheplayer.velox4j.test.SampleQueryTests;
import io.github.zhztheplayer.velox4j.test.TpchTests;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
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
  public void testAggregate() {
    final QueryExecutor queryExecutor = new QueryExecutor(memoryManager);
    final File file = TpchTests.Table.NATION.file();
    final RowType outputType = TpchTests.Table.NATION.schema();
    final TableScanNode scanNode = new TableScanNode(
        "id-1",
        outputType,
        new HiveTableHandle(
            "connector-hive",
            "tab-1",
            false,
            Collections.emptyList(),
            null,
            outputType,
            Collections.emptyMap()
        ),
        toAssignments(outputType)
    );
    final List<BoundSplit> splits = List.of(
        new BoundSplit(
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
        )
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
    final UpIterator itr = queryExecutor.execute(query);
    UpIteratorTests.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0,
            "n_regionkey\tcnt\n" +
                "0\t50\n" +
                "1\t47\n" +
                "4\t58\n" +
                "3\t77\n" +
                "2\t68\n")
        .run();
    queryExecutor.close();
  }

  @Test
  public void testExternalStream() {
    final QueryExecutor queryExecutor = new QueryExecutor(memoryManager);
    final String json = SampleQueryTests.readQueryJson();
    final UpIterator sampleIn = queryExecutor.execute(json);
    final DownIterator down = new DownIterator(sampleIn);
    final ExternalStream es = queryExecutor.bindDownIterator(down);
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
    final String queryJson = Serde.toPrettyJson(query);
    final UpIterator out = queryExecutor.execute(queryJson);
    SampleQueryTests.assertIterator(out);
    queryExecutor.close();
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
}

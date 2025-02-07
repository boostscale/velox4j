package io.github.zhztheplayer.velox4j.plan;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.aggregate.AggregateStep;
import io.github.zhztheplayer.velox4j.connector.Assignment;
import io.github.zhztheplayer.velox4j.connector.ColumnType;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.connector.FileFormat;
import io.github.zhztheplayer.velox4j.connector.HiveColumnHandle;
import io.github.zhztheplayer.velox4j.connector.HiveConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.HiveTableHandle;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.iterator.ExternalStream;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.query.BoundSplit;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.test.Resources;
import io.github.zhztheplayer.velox4j.test.SampleQueryTests;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

public class PlanNodeTest {
  @BeforeClass
  public static void beforeClass() {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testSanity() {
    // TODO: Cleanup the code.
    // TODO: Add assertions.
    final JniApi jniApi = JniApi.create();
    final File file = Resources.copyResourceToTmp("data/tpch-sf0.1/nation/nation.parquet");
    final RowType outputType = new RowType(List.of("n_nationkey", "n_name", "n_regionkey", "n_comment"),
        List.of(new BigIntType(), new VarCharType(), new BigIntType(), new VarCharType()));
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
    final Query query = new Query(aggregationNode, splits);
    final String queryJson = Serde.toPrettyJson(query);
    final UpIterator itr = jniApi.executeQuery(queryJson);
    final BufferAllocator alloc = new RootAllocator();
    while (itr.hasNext()) {
      final RowVector vector = itr.next();
      System.out.println(RowVectors.toString(alloc, vector));
    }
    jniApi.close();
  }

  @Test
  public void testExternalStream() {
    // TODO: Cleanup the code.
    // TODO: Add assertions.
    final JniApi jniApi = JniApi.create();
    final String json = SampleQueryTests.readQueryJson();
    final UpIterator sampleIn = jniApi.executeQuery(json);
    final DownIterator down = new DownIterator(sampleIn);
    final ExternalStream es = jniApi.downIteratorAsExternalStream(down);
    final TableScanNode scanNode = new TableScanNode(
        "id-1",
        new RowType(List.of(), List.of()),
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
    final Query query = new Query(scanNode, splits);
    final String queryJson = Serde.toPrettyJson(query);
    final UpIterator out = jniApi.executeQuery(queryJson);
    SampleQueryTests.assertIterator(out);
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
}

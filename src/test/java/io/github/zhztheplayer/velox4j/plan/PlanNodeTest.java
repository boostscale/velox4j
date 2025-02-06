package io.github.zhztheplayer.velox4j.plan;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.collection.Streams;
import io.github.zhztheplayer.velox4j.connector.Assignment;
import io.github.zhztheplayer.velox4j.connector.ColumnType;
import io.github.zhztheplayer.velox4j.connector.FileFormat;
import io.github.zhztheplayer.velox4j.connector.HiveColumnHandle;
import io.github.zhztheplayer.velox4j.connector.HiveConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.HiveTableHandle;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.query.BoundSplit;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.test.Resources;
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
import java.util.stream.Collectors;

public class PlanNodeTest {
  @BeforeClass
  public static void beforeClass() {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testTableScanNode() {
    // TODO: Cleanup the code.
    // TODO: Add assertions.
    final JniApi jniApi = JniApi.create();
    final File file = Resources.copyResourceToTmp("data/tpch-sf0.1/nation/nation.parquet");
    final RowType outputType = new RowType(List.of("n_nationkey", "n_name", "n_regionkey", "n_comment"),
        List.of(new BigIntType(), new VarCharType(), new BigIntType(), new VarCharType()));
    final PlanNode node = new TableScanNode(
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
            node.getId(),
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
    final Query query = new Query(node, splits);
    final String queryJson = Serde.toPrettyJson(query);
    final List<RowVector> vectors = Streams.fromIterator(jniApi.executeQuery(queryJson)).collect(Collectors.toList());
    final BufferAllocator alloc = new RootAllocator();
    for (RowVector vector : vectors) {
      System.out.println(RowVectors.toString(alloc, vector));
    }
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

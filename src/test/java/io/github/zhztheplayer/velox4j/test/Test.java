package io.github.zhztheplayer.velox4j.test;

import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.connector.FileFormat;
import io.github.zhztheplayer.velox4j.connector.HiveConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.HiveTableHandle;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.query.BoundSplit;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.table.Table;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;


public class Test {
  public static void main(String[] args) {
    // 1. Define the plan output schema:
    final RowType outputType = new RowType(List.of(
        "n_nationkey",
        "n_name",
        "n_regionkey",
        "n_comment"
    ), List.of(
        new BigIntType(),
        new VarCharType(),
        new BigIntType(),
        new VarCharType()
    ));

// 2. Create a table scan node.
    final TableScanNode scanNode = new TableScanNode(
        "plan-id-1",
        outputType,
        new HiveTableHandle(
            "connector-hive",
            "table-1",
            false,
            Collections.emptyList(),
            null,
            outputType,
            Collections.emptyMap()
        ),
        toAssignments(outputType)
    );

// 3. Create a split associating with the table scan node, this makes
// the scan read a local file "/tmp/nation.parquet".
    final BoundSplit split = new BoundSplit(
        scanNode.getId(),
        -1,
        new HiveConnectorSplit(
            "connector-hive",
            0,
            false,
            "/tmp/nation.parquet",
            FileFormat.PARQUET,
            0,
            new File("/tmp/nation.parquet").length(),
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

// 4. Build the query.
    final Query query = new Query(scanNode, List.of(split), Config.empty(), ConnectorConfig.empty());

// 5. Create a JNI session.
    final MemoryManager memoryManager = MemoryManager.create(AllocationListener.NOOP);
    final JniApi jniApi = JniApi.create(memoryManager);

// 6. Execute the query.
    final UpIterator itr = query.execute(jniApi);

// 7. Collect and print results.
    while (itr.hasNext()) {
      final RowVector rowVector = itr.next();
      final Table arrowTable = Arrow.toArrowTable(new RootAllocator(), rowVector);
      System.out.println(arrowTable.toVectorSchemaRoot().contentToTSVString());
    }

// 8. Close the JNI session.
    jniApi.close();
    memoryManager.close();
  }
}

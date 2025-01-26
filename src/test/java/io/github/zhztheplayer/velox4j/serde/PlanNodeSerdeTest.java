package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.connector.ConnectorTableHandle;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.RowType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class PlanNodeSerdeTest {
  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testTableScanNode() {
    final ConnectorTableHandle handle = SerdeTests.newSampleHiveTableHandle();
    final PlanNode scan = new TableScanNode("id-1", new RowType(List.of("foo", "bar"),
        List.of(new IntegerType(), new IntegerType())), handle, Collections.emptyList());
    SerdeTests.testVeloxBeanRoundTrip(scan);
  }
}

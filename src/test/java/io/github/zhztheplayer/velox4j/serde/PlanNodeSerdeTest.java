package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.aggregate.AggregateStep;
import io.github.zhztheplayer.velox4j.connector.ConnectorTableHandle;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.plan.AggregationNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.sort.SortOrder;
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
  public void testSortOrder() {
    final SortOrder order = new SortOrder(true, true);
    SerdeTests.testJavaBeanRoundTrip(order);
  }

  @Test
  public void testAggregateStep() {
    SerdeTests.testJavaBeanRoundTrip(AggregateStep.INTERMEDIATE);
  }

  @Test
  public void testAggregate() {
    final Aggregate aggregate = SerdeTests.newSampleAggregate();
    SerdeTests.testJavaBeanRoundTrip(aggregate);
  }

  @Test
  public void testTableScanNode() {
    final PlanNode scan = SerdeTests.newSampleTableScanNode();
    SerdeTests.testVeloxBeanRoundTrip(scan);
  }

  @Test
  public void testAggregationNode() {
    final PlanNode scan = SerdeTests.newSampleTableScanNode();
    final Aggregate aggregate = SerdeTests.newSampleAggregate();
    final AggregationNode aggregationNode = new AggregationNode(
        "id-1",
        AggregateStep.PARTIAL,
        List.of(FieldAccessTypedExpr.create(new IntegerType(), "foo")),
        List.of(FieldAccessTypedExpr.create(new IntegerType(), "foo")),
        List.of("sum"),
        List.of(aggregate),
        true,
        List.of(scan),
        new RowType(List.of("foo"), List.of(new IntegerType())),
        FieldAccessTypedExpr.create(new IntegerType(), "foo"),
        List.of(0)
    );
    SerdeTests.testVeloxBeanRoundTrip(aggregationNode);
  }
}

package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.aggregate.AggregateStep;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.join.JoinType;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.AggregationNode;
import io.github.zhztheplayer.velox4j.plan.FilterNode;
import io.github.zhztheplayer.velox4j.plan.HashJoinNode;
import io.github.zhztheplayer.velox4j.plan.LimitNode;
import io.github.zhztheplayer.velox4j.plan.OrderByNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;
import io.github.zhztheplayer.velox4j.plan.ValuesNode;
import io.github.zhztheplayer.velox4j.sort.SortOrder;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.variant.BooleanValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class PlanNodeSerdeTest {
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
  public void testJoinType() {
    SerdeTests.testJavaBeanRoundTrip(JoinType.LEFT_SEMI_FILTER);
  }

  @Test
  public void testValuesNode() {
    final Session session = Velox4j.newSession(memoryManager);
    final PlanNode values = ValuesNode.create("id-1",
        List.of(SerdeTests.newSampleRowVector(session)), true, 1);
    SerdeTests.testVeloxSerializableRoundTrip(values);
    session.close();
  }

  @Test
  public void testTableScanNode() {
    final PlanNode scan = SerdeTests.newSampleTableScanNode("id-1",
        SerdeTests.newSampleOutputType());
    SerdeTests.testVeloxSerializableRoundTrip(scan);
  }

  @Test
  public void testAggregationNode() {
    final PlanNode scan = SerdeTests.newSampleTableScanNode("id-1",
        SerdeTests.newSampleOutputType());
    final Aggregate aggregate = SerdeTests.newSampleAggregate();
    final AggregationNode aggregationNode = new AggregationNode(
        "id-2",
        AggregateStep.PARTIAL,
        List.of(FieldAccessTypedExpr.create(new IntegerType(), "foo")),
        List.of(FieldAccessTypedExpr.create(new IntegerType(), "foo")),
        List.of("sum"),
        List.of(aggregate),
        true,
        List.of(scan),
        FieldAccessTypedExpr.create(new IntegerType(), "foo"),
        List.of(0)
    );
    SerdeTests.testVeloxSerializableRoundTrip(aggregationNode);
  }

  @Test
  public void testProjectNode() {
    final PlanNode scan = SerdeTests.newSampleTableScanNode("id-1",
        SerdeTests.newSampleOutputType());
    final ProjectNode projectNode = new ProjectNode("id-2", List.of(scan),
        List.of("foo"),
        List.of(FieldAccessTypedExpr.create(new IntegerType(), "foo")));
    SerdeTests.testVeloxSerializableRoundTrip(projectNode);
  }

  @Test
  public void testFilterNode() {
    final PlanNode scan = SerdeTests.newSampleTableScanNode("id-1",
        SerdeTests.newSampleOutputType());
    final FilterNode filterNode = new FilterNode("id-2", List.of(scan),
        ConstantTypedExpr.create(new BooleanValue(true)));
    SerdeTests.testVeloxSerializableRoundTrip(filterNode);
  }

  @Test
  public void testHashJoinNode() {
    final PlanNode scan1 = SerdeTests.newSampleTableScanNode("id-1",
        new RowType(List.of("foo1", "bar1"),
            List.of(new IntegerType(), new IntegerType())));
    final PlanNode scan2 = SerdeTests.newSampleTableScanNode("id-2",
        new RowType(List.of("foo2", "bar2"),
            List.of(new IntegerType(), new IntegerType())));
    final PlanNode joinNode = new HashJoinNode(
        "id-3",
        JoinType.INNER,
        List.of(FieldAccessTypedExpr.create(new IntegerType(), "foo1")),
        List.of(FieldAccessTypedExpr.create(new IntegerType(), "foo2")),
        ConstantTypedExpr.create(new BooleanValue(true)),
        scan1,
        scan2,
        new RowType(List.of("foo1", "bar1", "foo2", "bar2"),
            List.of(new IntegerType(), new IntegerType(), new IntegerType(), new IntegerType())),
        false
    );
    SerdeTests.testVeloxSerializableRoundTrip(joinNode);
  }

  @Test
  public void testOrderByNode() {
    final PlanNode scan = SerdeTests.newSampleTableScanNode("id-1",
        SerdeTests.newSampleOutputType());
    final OrderByNode orderByNode = new OrderByNode("id-2", List.of(scan),
        List.of(FieldAccessTypedExpr.create(new IntegerType(), "foo1")),
        List.of(new SortOrder(true, false)),
        false);
    SerdeTests.testVeloxSerializableRoundTrip(orderByNode);
  }

  @Test
  public void testLimitNode() {
    final PlanNode scan = SerdeTests.newSampleTableScanNode("id-1",
        SerdeTests.newSampleOutputType());
    final LimitNode limitNode = new LimitNode("id-2", List.of(scan), 5, 3, false);
    SerdeTests.testVeloxSerializableRoundTrip(limitNode);
  }
}

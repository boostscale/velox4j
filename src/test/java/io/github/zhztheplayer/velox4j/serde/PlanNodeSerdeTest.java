/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package io.github.zhztheplayer.velox4j.serde;

import com.google.common.collect.ImmutableList;
import org.junit.*;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.aggregate.AggregateStep;
import io.github.zhztheplayer.velox4j.connector.CommitStrategy;
import io.github.zhztheplayer.velox4j.data.BaseVectorTests;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.join.JoinType;
import io.github.zhztheplayer.velox4j.memory.BytesAllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.AggregationNode;
import io.github.zhztheplayer.velox4j.plan.FilterNode;
import io.github.zhztheplayer.velox4j.plan.HashJoinNode;
import io.github.zhztheplayer.velox4j.plan.LimitNode;
import io.github.zhztheplayer.velox4j.plan.OrderByNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;
import io.github.zhztheplayer.velox4j.plan.TableWriteNode;
import io.github.zhztheplayer.velox4j.plan.ValuesNode;
import io.github.zhztheplayer.velox4j.plan.WindowNode;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.sort.SortOrder;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.variant.BooleanValue;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;
import io.github.zhztheplayer.velox4j.window.BoundType;
import io.github.zhztheplayer.velox4j.window.WindowFrame;
import io.github.zhztheplayer.velox4j.window.WindowFunction;
import io.github.zhztheplayer.velox4j.window.WindowType;

public class PlanNodeSerdeTest {
  private static BytesAllocationListener allocationListener;
  private static MemoryManager memoryManager;
  private static Session session;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
    allocationListener = new BytesAllocationListener();
    memoryManager = Velox4j.newMemoryManager(allocationListener);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    memoryManager.close();
    Assert.assertEquals(0, allocationListener.currentBytes());
  }

  @Before
  public void setUp() throws Exception {
    session = Velox4j.newSession(memoryManager);
  }

  @After
  public void tearDown() throws Exception {
    session.close();
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

  // Ignored by https://github.com/velox4j/velox4j/issues/104.
  @Ignore
  public void testValuesNode() {
    // The case fails in debug build. Should investigate.
    final PlanNode values =
        ValuesNode.create(
            "id-1", ImmutableList.of(BaseVectorTests.newSampleRowVector(session)), true, 1);
    SerdeTests.testISerializableRoundTrip(values);
  }

  @Test
  public void testTableScanNode() {
    final PlanNode scan =
        SerdeTests.newSampleTableScanNode("id-1", SerdeTests.newSampleOutputType());
    SerdeTests.testISerializableRoundTrip(scan);
  }

  @Test
  public void testAggregationNode() {
    final AggregationNode aggregationNode = SerdeTests.newSampleAggregationNode("id-2", "id-1");
    SerdeTests.testISerializableRoundTrip(aggregationNode);
  }

  @Test
  public void testProjectNode() {
    final PlanNode scan =
        SerdeTests.newSampleTableScanNode("id-1", SerdeTests.newSampleOutputType());
    final ProjectNode projectNode =
        new ProjectNode(
            "id-2",
            ImmutableList.of(scan),
            ImmutableList.of("foo"),
            ImmutableList.of(FieldAccessTypedExpr.create(new IntegerType(), "foo")));
    SerdeTests.testISerializableRoundTrip(projectNode);
  }

  @Test
  public void testFilterNode() {
    final PlanNode scan =
        SerdeTests.newSampleTableScanNode("id-1", SerdeTests.newSampleOutputType());
    final FilterNode filterNode =
        new FilterNode(
            "id-2",
            ImmutableList.of(scan),
            ConstantTypedExpr.create(new BooleanType(), new BooleanValue(true)));
    SerdeTests.testISerializableRoundTrip(filterNode);
  }

  @Test
  public void testHashJoinNode() {
    final PlanNode scan1 =
        SerdeTests.newSampleTableScanNode(
            "id-1",
            new RowType(
                ImmutableList.of("foo1", "bar1"),
                ImmutableList.of(new IntegerType(), new IntegerType())));
    final PlanNode scan2 =
        SerdeTests.newSampleTableScanNode(
            "id-2",
            new RowType(
                ImmutableList.of("foo2", "bar2"),
                ImmutableList.of(new IntegerType(), new IntegerType())));
    final PlanNode joinNode =
        new HashJoinNode(
            "id-3",
            JoinType.INNER,
            ImmutableList.of(FieldAccessTypedExpr.create(new IntegerType(), "foo1")),
            ImmutableList.of(FieldAccessTypedExpr.create(new IntegerType(), "foo2")),
            ConstantTypedExpr.create(new BooleanType(), new BooleanValue(true)),
            scan1,
            scan2,
            new RowType(
                ImmutableList.of("foo1", "bar1", "foo2", "bar2"),
                ImmutableList.of(
                    new IntegerType(), new IntegerType(), new IntegerType(), new IntegerType())),
            false);
    SerdeTests.testISerializableRoundTrip(joinNode);
  }

  @Test
  public void testOrderByNode() {
    final PlanNode scan =
        SerdeTests.newSampleTableScanNode("id-1", SerdeTests.newSampleOutputType());
    final OrderByNode orderByNode =
        new OrderByNode(
            "id-2",
            ImmutableList.of(scan),
            ImmutableList.of(FieldAccessTypedExpr.create(new IntegerType(), "foo1")),
            ImmutableList.of(new SortOrder(true, false)),
            false);
    SerdeTests.testISerializableRoundTrip(orderByNode);
  }

  @Test
  public void testLimitNode() {
    final PlanNode scan =
        SerdeTests.newSampleTableScanNode("id-1", SerdeTests.newSampleOutputType());
    final LimitNode limitNode = new LimitNode("id-2", ImmutableList.of(scan), 5, 3, false);
    SerdeTests.testISerializableRoundTrip(limitNode);
  }

  @Test
  public void testTableWriteNode() {
    final RowType rowType = SerdeTests.newSampleOutputType();
    final PlanNode scan = SerdeTests.newSampleTableScanNode("id-1", rowType);
    final TableWriteNode tableWriteNode =
        new TableWriteNode(
            "id-2",
            rowType,
            rowType.getNames(),
            SerdeTests.newSampleAggregationNode("id-4", "id-3"),
            "connector-1",
            SerdeTests.newSampleHiveInsertTableHandle(),
            true,
            rowType,
            CommitStrategy.TASK_COMMIT,
            ImmutableList.of(scan));
    SerdeTests.testISerializableRoundTrip(tableWriteNode);
  }

  @Test
  public void testWindowType() {
    SerdeTests.testJavaBeanRoundTrip(WindowType.ROWS);
  }

  @Test
  public void testBoundType() {
    SerdeTests.testJavaBeanRoundTrip(BoundType.CURRENT_ROW);
  }

  @Test
  public void testWindowFrame() {
    final WindowFrame frame =
        new WindowFrame(
            WindowType.ROWS,
            BoundType.UNBOUNDED_PRECEDING,
            ConstantTypedExpr.create(new IntegerType(), new IntegerValue(100)),
            BoundType.CURRENT_ROW,
            ConstantTypedExpr.create(new IntegerType(), new IntegerValue(200)));
    SerdeTests.testJavaBeanRoundTrip(frame);
  }

  @Test
  public void testWindowFunction() {
    final WindowFunction windowFunction = SerdeTests.newSampleWindowFunction();
    SerdeTests.testJavaBeanRoundTrip(windowFunction);
  }

  @Test
  public void testWindowNode() {
    final WindowNode windowNode = SerdeTests.newSampleWindowNode("id-1", "id-2");
    SerdeTests.testISerializableRoundTrip(windowNode);
  }
}

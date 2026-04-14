/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.boostscale.velox4j.plan;

import com.google.common.collect.ImmutableList;
import org.junit.*;

import org.boostscale.velox4j.Velox4j;
import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.ConstantTypedExpr;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.join.JoinType;
import org.boostscale.velox4j.memory.BytesAllocationListener;
import org.boostscale.velox4j.memory.MemoryManager;
import org.boostscale.velox4j.serde.SerdeTests;
import org.boostscale.velox4j.session.Session;
import org.boostscale.velox4j.test.Velox4jTests;
import org.boostscale.velox4j.type.BooleanType;
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.variant.BooleanValue;
import org.boostscale.velox4j.variant.IntegerValue;

public class PlanNodeToStringTest {
  private static BytesAllocationListener allocationListener;
  private static MemoryManager memoryManager;
  private Session session;

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
  public void setUp() {
    session = Velox4j.newSession(memoryManager);
  }

  @After
  public void tearDown() {
    session.close();
  }

  @Test
  public void testTableScanNodeSimple() {
    PlanNode scan = SerdeTests.newSampleTableScanNode("scan-1", SerdeTests.newSampleOutputType());
    String result = scan.toFormatString(session, false, false);
    Assert.assertNotNull(result);
    Assert.assertTrue("Got: " + result, result.contains("scan-1"));
  }

  @Test
  public void testTableScanNodeDetailed() {
    PlanNode scan = SerdeTests.newSampleTableScanNode("scan-1", SerdeTests.newSampleOutputType());
    String result = scan.toFormatString(session, true, false);
    Assert.assertNotNull(result);
    Assert.assertTrue("Got: " + result, result.contains("scan-1"));
    // Detailed mode includes output type info
    Assert.assertTrue("Got: " + result, result.contains("->"));
  }

  @Test
  public void testFilterNodeRecursive() {
    PlanNode scan = SerdeTests.newSampleTableScanNode("scan-1", SerdeTests.newSampleOutputType());
    FilterNode filter =
        new FilterNode(
            "filter-1",
            ImmutableList.of(scan),
            ConstantTypedExpr.create(new BooleanType(), new BooleanValue(true)));
    String result = filter.toFormatString(session, true, true);
    Assert.assertNotNull(result);
    // Recursive: should contain both nodes
    Assert.assertTrue("Should contain filter-1, got: " + result, result.contains("filter-1"));
    Assert.assertTrue("Should contain scan-1, got: " + result, result.contains("scan-1"));
    // Recursive output has child indented under parent
    Assert.assertTrue("Should have indentation, got: " + result, result.contains("  --"));
  }

  @Test
  public void testProjectNodeDetailed() {
    PlanNode scan = SerdeTests.newSampleTableScanNode("scan-1", SerdeTests.newSampleOutputType());
    ProjectNode project =
        new ProjectNode(
            "proj-1",
            ImmutableList.of(scan),
            ImmutableList.of("result"),
            ImmutableList.of(
                new CallTypedExpr(
                    new IntegerType(),
                    ImmutableList.of(
                        FieldAccessTypedExpr.create(new IntegerType(), "foo"),
                        ConstantTypedExpr.create(new IntegerType(), new IntegerValue(2))),
                    "multiply")));
    String result = project.toFormatString(session, true, true);
    Assert.assertNotNull(result);
    Assert.assertTrue("Should contain proj-1, got: " + result, result.contains("proj-1"));
    Assert.assertTrue("Should contain multiply, got: " + result, result.contains("multiply"));
  }

  @Test
  public void testHashJoinNodeDetailed() {
    RowType leftType =
        new RowType(
            ImmutableList.of("l_id", "l_value"),
            ImmutableList.of(new IntegerType(), new IntegerType()));
    RowType rightType =
        new RowType(
            ImmutableList.of("r_id", "r_score"),
            ImmutableList.of(new IntegerType(), new IntegerType()));
    RowType outputType =
        new RowType(
            ImmutableList.of("l_id", "l_value", "r_id", "r_score"),
            ImmutableList.of(
                new IntegerType(), new IntegerType(), new IntegerType(), new IntegerType()));

    PlanNode leftScan = SerdeTests.newSampleTableScanNode("left-scan", leftType);
    PlanNode rightScan = SerdeTests.newSampleTableScanNode("right-scan", rightType);

    HashJoinNode join =
        new HashJoinNode(
            "join-1",
            JoinType.INNER,
            ImmutableList.of(FieldAccessTypedExpr.create(new IntegerType(), "l_id")),
            ImmutableList.of(FieldAccessTypedExpr.create(new IntegerType(), "r_id")),
            null,
            leftScan,
            rightScan,
            outputType,
            false,
            false);

    String result = join.toFormatString(session, true, true);
    Assert.assertNotNull(result);
    Assert.assertTrue("Should contain INNER, got: " + result, result.contains("INNER"));
    Assert.assertTrue("Should contain left-scan, got: " + result, result.contains("left-scan"));
    Assert.assertTrue("Should contain right-scan, got: " + result, result.contains("right-scan"));
  }
}

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

import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.CastTypedExpr;
import org.boostscale.velox4j.expression.ConstantTypedExpr;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.join.JoinType;
import org.boostscale.velox4j.serde.SerdeTests;
import org.boostscale.velox4j.test.Velox4jTests;
import org.boostscale.velox4j.type.BigIntType;
import org.boostscale.velox4j.type.BooleanType;
import org.boostscale.velox4j.type.IntegerType;
import org.boostscale.velox4j.type.RowType;
import org.boostscale.velox4j.type.VarCharType;
import org.boostscale.velox4j.variant.BooleanValue;
import org.boostscale.velox4j.variant.IntegerValue;

public class PlanNodeToStringTest {

  private static final String SAMPLE_SCAN_DETAILS =
      "table: tab-1,"
          + " range filters: [(complex_type[1].id,"
          + " Filter(AlwaysTrue, deterministic, with nulls))],"
          + " remaining filter: (always_true()),"
          + " data columns: ROW<foo:INTEGER,bar:INTEGER>,"
          + " table parameters: [tk:tv]";

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
  }

  @Test
  public void testTableScanNodeSimple() {
    PlanNode scan = SerdeTests.newSampleTableScanNode("scan-1", SerdeTests.newSampleOutputType());
    Assert.assertEquals("-- TableScan[scan-1]\n", scan.toFormatString(false, false));
  }

  @Test
  public void testTableScanNodeDetailed() {
    PlanNode scan = SerdeTests.newSampleTableScanNode("scan-1", SerdeTests.newSampleOutputType());
    Assert.assertEquals(
        "-- TableScan[scan-1][" + SAMPLE_SCAN_DETAILS + "] -> foo:INTEGER, bar:INTEGER\n",
        scan.toFormatString(true, false));
  }

  @Test
  public void testFilterNodeRecursive() {
    PlanNode scan = SerdeTests.newSampleTableScanNode("scan-1", SerdeTests.newSampleOutputType());
    FilterNode filter =
        new FilterNode(
            "filter-1",
            ImmutableList.of(scan),
            ConstantTypedExpr.create(new BooleanType(), new BooleanValue(true)));
    Assert.assertEquals(
        "-- Filter[filter-1][expression: true] -> foo:INTEGER, bar:INTEGER\n"
            + "  -- TableScan[scan-1]["
            + SAMPLE_SCAN_DETAILS
            + "] -> foo:INTEGER, bar:INTEGER\n",
        filter.toFormatString(true, true));
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
    Assert.assertEquals(
        "-- Project[proj-1][expressions: (result:INTEGER, multiply(\"foo\",2))] -> result:INTEGER\n"
            + "  -- TableScan[scan-1]["
            + SAMPLE_SCAN_DETAILS
            + "] -> foo:INTEGER, bar:INTEGER\n",
        project.toFormatString(true, true));
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

    String leftScanDetails =
        "table: tab-1,"
            + " range filters: [(complex_type[1].id,"
            + " Filter(AlwaysTrue, deterministic, with nulls))],"
            + " remaining filter: (always_true()),"
            + " data columns: ROW<l_id:INTEGER,l_value:INTEGER>,"
            + " table parameters: [tk:tv]";
    String rightScanDetails =
        "table: tab-1,"
            + " range filters: [(complex_type[1].id,"
            + " Filter(AlwaysTrue, deterministic, with nulls))],"
            + " remaining filter: (always_true()),"
            + " data columns: ROW<r_id:INTEGER,r_score:INTEGER>,"
            + " table parameters: [tk:tv]";

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
            false,
            false);

    Assert.assertEquals(
        "-- HashJoin[join-1][INNER l_id=r_id]"
            + " -> l_id:INTEGER, l_value:INTEGER, r_id:INTEGER, r_score:INTEGER\n"
            + "  -- TableScan[left-scan]["
            + leftScanDetails
            + "] -> l_id:INTEGER, l_value:INTEGER\n"
            + "  -- TableScan[right-scan]["
            + rightScanDetails
            + "] -> r_id:INTEGER, r_score:INTEGER\n",
        join.toFormatString(true, true));
  }

  @Test
  public void testFilterWithComplexExpression() {
    // WHERE age >= 18 AND length(name) > 0
    PlanNode scan =
        SerdeTests.newSampleTableScanNode(
            "scan-1",
            new RowType(
                ImmutableList.of("age", "name"),
                ImmutableList.of(new IntegerType(), new VarCharType())));

    CallTypedExpr ageCheck =
        new CallTypedExpr(
            new BooleanType(),
            ImmutableList.of(
                FieldAccessTypedExpr.create(new IntegerType(), "age"),
                ConstantTypedExpr.create(new IntegerType(), new IntegerValue(18))),
            "gte");
    CallTypedExpr lengthCheck =
        new CallTypedExpr(
            new BooleanType(),
            ImmutableList.of(
                new CallTypedExpr(
                    new IntegerType(),
                    ImmutableList.of(FieldAccessTypedExpr.create(new VarCharType(), "name")),
                    "length"),
                ConstantTypedExpr.create(new IntegerType(), new IntegerValue(0))),
            "gt");
    CallTypedExpr andExpr =
        new CallTypedExpr(new BooleanType(), ImmutableList.of(ageCheck, lengthCheck), "and");

    FilterNode filter = new FilterNode("filter-1", ImmutableList.of(scan), andExpr);
    Assert.assertEquals(
        "-- Filter[filter-1][expression: and(gte(\"age\",18),gt(length(\"name\"),0))]"
            + " -> age:INTEGER, name:VARCHAR\n",
        filter.toFormatString(true, false));
  }

  @Test
  public void testProjectWithCastExpression() {
    // SELECT CAST(foo AS BIGINT) AS foo_big, CAST(bar AS VARCHAR) AS bar_str
    PlanNode scan = SerdeTests.newSampleTableScanNode("scan-1", SerdeTests.newSampleOutputType());
    ProjectNode project =
        new ProjectNode(
            "proj-1",
            ImmutableList.of(scan),
            ImmutableList.of("foo_big", "bar_str"),
            ImmutableList.of(
                CastTypedExpr.create(
                    new BigIntType(), FieldAccessTypedExpr.create(new IntegerType(), "foo"), false),
                CastTypedExpr.create(
                    new VarCharType(),
                    FieldAccessTypedExpr.create(new IntegerType(), "bar"),
                    true)));

    Assert.assertEquals(
        "-- Project[proj-1][expressions:"
            + " (foo_big:BIGINT, cast(\"foo\" as BIGINT)),"
            + " (bar_str:VARCHAR, try_cast(\"bar\" as VARCHAR))]"
            + " -> foo_big:BIGINT, bar_str:VARCHAR\n",
        project.toFormatString(true, false));
  }

  @Test
  public void testHashJoinWithPostFilter() {
    RowType leftType =
        new RowType(
            ImmutableList.of("l_id", "l_val"),
            ImmutableList.of(new IntegerType(), new IntegerType()));
    RowType rightType =
        new RowType(
            ImmutableList.of("r_id", "r_val"),
            ImmutableList.of(new IntegerType(), new IntegerType()));
    RowType outputType =
        new RowType(
            ImmutableList.of("l_id", "l_val", "r_id", "r_val"),
            ImmutableList.of(
                new IntegerType(), new IntegerType(), new IntegerType(), new IntegerType()));

    PlanNode leftScan = SerdeTests.newSampleTableScanNode("left-scan", leftType);
    PlanNode rightScan = SerdeTests.newSampleTableScanNode("right-scan", rightType);

    // Post-join filter: l_val > r_val
    CallTypedExpr postFilter =
        new CallTypedExpr(
            new BooleanType(),
            ImmutableList.of(
                FieldAccessTypedExpr.create(new IntegerType(), "l_val"),
                FieldAccessTypedExpr.create(new IntegerType(), "r_val")),
            "gt");

    HashJoinNode join =
        new HashJoinNode(
            "join-1",
            JoinType.LEFT,
            ImmutableList.of(FieldAccessTypedExpr.create(new IntegerType(), "l_id")),
            ImmutableList.of(FieldAccessTypedExpr.create(new IntegerType(), "r_id")),
            postFilter,
            leftScan,
            rightScan,
            outputType,
            false,
            false,
            false);

    Assert.assertEquals(
        "-- HashJoin[join-1][LEFT l_id=r_id, filter: gt(\"l_val\",\"r_val\")]"
            + " -> l_id:INTEGER, l_val:INTEGER, r_id:INTEGER, r_val:INTEGER\n",
        join.toFormatString(true, false));
  }
}

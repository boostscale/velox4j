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
package org.boostscale.velox4j.eval;

import com.google.common.collect.ImmutableList;
import org.junit.*;

import org.boostscale.velox4j.Velox4j;
import org.boostscale.velox4j.config.Config;
import org.boostscale.velox4j.config.ConnectorConfig;
import org.boostscale.velox4j.data.BaseVector;
import org.boostscale.velox4j.data.BaseVectorTests;
import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.data.SelectivityVector;
import org.boostscale.velox4j.expression.CallTypedExpr;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;
import org.boostscale.velox4j.memory.BytesAllocationListener;
import org.boostscale.velox4j.memory.MemoryManager;
import org.boostscale.velox4j.session.Session;
import org.boostscale.velox4j.test.ResourceTests;
import org.boostscale.velox4j.test.Velox4jTests;
import org.boostscale.velox4j.type.BigIntType;

public class EvaluationTest {
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
  public void testFieldAccess() {
    final RowVector input = BaseVectorTests.newSampleRowVector(session);
    final int size = input.getSize();
    final SelectivityVector sv = session.selectivityVectorOps().create(size);
    final Evaluation expr =
        new Evaluation(
            FieldAccessTypedExpr.create(new BigIntType(), "c0"),
            Config.empty(),
            ConnectorConfig.empty());
    final Evaluator evaluator = session.evaluationOps().createEvaluator(expr);
    final BaseVector out = evaluator.eval(sv, input);
    final String outString = out.toString();
    Assert.assertEquals(
        ResourceTests.readResourceAsString("eval-output/field-access-1.txt"), outString);
  }

  @Test
  public void testMultipleEvalCalls() {
    final RowVector input = BaseVectorTests.newSampleRowVector(session);
    final int size = input.getSize();
    final SelectivityVector sv = session.selectivityVectorOps().create(size);
    final Evaluation expr =
        new Evaluation(
            FieldAccessTypedExpr.create(new BigIntType(), "c0"),
            Config.empty(),
            ConnectorConfig.empty());
    final Evaluator evaluator = session.evaluationOps().createEvaluator(expr);
    final String expected = ResourceTests.readResourceAsString("eval-output/field-access-1.txt");
    for (int i = 0; i < 10; i++) {
      final BaseVector out = evaluator.eval(sv, input);
      final String outString = out.toString();
      Assert.assertEquals(expected, outString);
    }
  }

  @Test
  public void testMultiply() {
    final RowVector input = BaseVectorTests.newSampleRowVector(session);
    final int size = input.getSize();
    final SelectivityVector sv = session.selectivityVectorOps().create(size);
    final Evaluation expr =
        new Evaluation(
            new CallTypedExpr(
                new BigIntType(),
                ImmutableList.of(
                    FieldAccessTypedExpr.create(new BigIntType(), "c0"),
                    FieldAccessTypedExpr.create(new BigIntType(), "a1")),
                "multiply"),
            Config.empty(),
            ConnectorConfig.empty());
    final Evaluator evaluator = session.evaluationOps().createEvaluator(expr);
    final BaseVector out = evaluator.eval(sv, input);
    final String outString = out.toString();
    Assert.assertEquals(
        ResourceTests.readResourceAsString("eval-output/multiply-1.txt"), outString);
  }
}

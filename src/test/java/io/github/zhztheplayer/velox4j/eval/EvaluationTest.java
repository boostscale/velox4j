package io.github.zhztheplayer.velox4j.eval;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.BaseVectorTests;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.SelectivityVector;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.test.ResourceTests;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import org.junit.*;

import java.util.List;

public class EvaluationTest {
  private static MemoryManager memoryManager;
  private static Session session;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
    memoryManager = MemoryManager.create(AllocationListener.NOOP);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    memoryManager.close();
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
    final Evaluation expr = new Evaluation(
        FieldAccessTypedExpr.create(new BigIntType(), "c0"),
        Config.empty(),
        ConnectorConfig.empty()
    );
    final Evaluator evaluator = session.evaluationOps().createEvaluator(expr);
    final BaseVector out = evaluator.eval(sv, input);
    final String outString = out.toString();
    Assert.assertEquals(
        ResourceTests.readResourceAsString("eval-output/field-access-1.txt"),
        outString);
  }

  @Test
  public void testMultipleEvalCalls() {
    final RowVector input = BaseVectorTests.newSampleRowVector(session);
    final int size = input.getSize();
    final SelectivityVector sv = session.selectivityVectorOps().create(size);
    final Evaluation expr = new Evaluation(
        FieldAccessTypedExpr.create(new BigIntType(), "c0"),
        Config.empty(),
        ConnectorConfig.empty()
    );
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
    final Evaluation expr = new Evaluation(
        new CallTypedExpr(new BigIntType(), List.of(
            FieldAccessTypedExpr.create(new BigIntType(), "c0"),
            FieldAccessTypedExpr.create(new BigIntType(), "a1")
        ), "multiply"),
        Config.empty(),
        ConnectorConfig.empty()
    );
    final Evaluator evaluator = session.evaluationOps().createEvaluator(expr);
    final BaseVector out = evaluator.eval(sv, input);
    final String outString = out.toString();
    Assert.assertEquals(
        ResourceTests.readResourceAsString("eval-output/multiply-1.txt"),
        outString);
  }
}

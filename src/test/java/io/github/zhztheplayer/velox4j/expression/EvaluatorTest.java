package io.github.zhztheplayer.velox4j.expression;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.BaseVectors;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.data.SelectivityVector;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.serde.SerdeTests;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.test.ResourceTests;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class EvaluatorTest {
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
  public void testFieldAccess() {
    final Session session = Velox4j.newSession(memoryManager);
    final RowVector input = SerdeTests.newSampleRowVector(session);
    final int size = BaseVectors.getSize(input);
    final SelectivityVector sv = session.selectivityVectorOps().create(size);
    final Expression expr = new Expression(
        FieldAccessTypedExpr.create(new BigIntType(), "c0"),
        Config.empty(),
        ConnectorConfig.empty()
    );
    final Evaluator evaluator = session.expressionOps().createEvaluator(expr);
    final BaseVector out = evaluator.eval(sv, input);
    final String outString = BaseVectors.toString(new RootAllocator(), out);
    Assert.assertEquals(
        ResourceTests.readResourceAsString("eval-output/field-access-1.txt"),
        outString);
    session.close();
  }

  @Test
  public void testMultipleEvalCalls() {
    final Session session = Velox4j.newSession(memoryManager);
    final RowVector input = SerdeTests.newSampleRowVector(session);
    final int size = BaseVectors.getSize(input);
    final SelectivityVector sv = session.selectivityVectorOps().create(size);
    final Expression expr = new Expression(
        FieldAccessTypedExpr.create(new BigIntType(), "c0"),
        Config.empty(),
        ConnectorConfig.empty()
    );
    final BufferAllocator alloc = new RootAllocator();
    final Evaluator evaluator = session.expressionOps().createEvaluator(expr);
    final String expected = ResourceTests.readResourceAsString("eval-output/field-access-1.txt");
    for (int i = 0; i < 10; i++) {
      final BaseVector out = evaluator.eval(sv, input);
      final String outString = BaseVectors.toString(alloc, out);
      Assert.assertEquals(expected, outString);
    }
    session.close();
  }

  @Test
  public void testMultiply() {
    final Session session = Velox4j.newSession(memoryManager);
    final RowVector input = SerdeTests.newSampleRowVector(session);
    final int size = BaseVectors.getSize(input);
    final SelectivityVector sv = session.selectivityVectorOps().create(size);
    final Expression expr = new Expression(
        new CallTypedExpr(new BigIntType(), List.of(
            FieldAccessTypedExpr.create(new BigIntType(), "c0"),
            FieldAccessTypedExpr.create(new BigIntType(), "a1")
        ), "multiply"),
        Config.empty(),
        ConnectorConfig.empty()
    );
    final Evaluator evaluator = session.expressionOps().createEvaluator(expr);
    final BaseVector out = evaluator.eval(sv, input);
    final String outString = BaseVectors.toString(new RootAllocator(), out);
    Assert.assertEquals(
        ResourceTests.readResourceAsString("eval-output/multiply-1.txt"),
        outString);
    session.close();
  }
}

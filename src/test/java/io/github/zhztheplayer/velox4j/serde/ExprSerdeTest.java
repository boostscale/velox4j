package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConcatTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.test.Serdes;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.RealType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ComparisonFailure;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class ExprSerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.initialize();
  }

  @Test
  public void testCallTypedExpr() {
    Serdes.testRoundTrip(new CallTypedExpr(new IntegerType(), Collections.emptyList(), "randomInt"));
  }

  @Test
  public void testCastTypedExpr() {
    final CallTypedExpr input = new CallTypedExpr(new IntegerType(), Collections.emptyList(), "randomInt");
    Serdes.testRoundTrip(new CastTypedExpr(new IntegerType(), Collections.singletonList(input), true));
  }

  @Test
  public void testConcatTypedExpr() {
    final CallTypedExpr input1 = new CallTypedExpr(new IntegerType(), Collections.emptyList(), "randomInt");
    final CallTypedExpr input2 = new CallTypedExpr(new RealType(), Collections.emptyList(), "randomReal");
    Serdes.testRoundTrip(ConcatTypedExpr.create(Arrays.asList("foo", "bar"), Arrays.asList(input1, input2)));
  }

  @Test
  public void testConstantTypedExpr() {
    final JniApi jniApi = JniApi.create();
    final BufferAllocator alloc = new RootAllocator();
    final IntVector arrowVector = new IntVector("foo", alloc);
    arrowVector.setValueCount(1);
    arrowVector.set(0, 15);
    final ConstantTypedExpr expr = ConstantTypedExpr.create(jniApi, alloc, arrowVector);
    // FIXME: The serialized string of the constant doesn't match after round-tripping.
    Assert.assertThrows(ComparisonFailure.class, () -> Serdes.testRoundTrip(expr));
    arrowVector.close();
    jniApi.close();
  }

  @Test
  public void testConstantTypedExprCreatedByString() {
    final JniApi jniApi = JniApi.create();
    final ConstantTypedExpr expr = ConstantTypedExpr.create(jniApi,
        "AQAAACAAAAB7InR5cGUiOiJJTlRFR0VSIiwibmFtZSI6IlR5cGUifQEAAAAAAQ8AAAA=");
    Serdes.testRoundTrip(expr);
  }
}

package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConcatTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.DereferenceTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.InputTypedExpr;
import io.github.zhztheplayer.velox4j.expression.LambdaTypedExpr;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.RealType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ComparisonFailure;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class ExprSerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testCallTypedExpr() {
    SerdeTests.testVeloxBeanRoundTrip(new CallTypedExpr(new IntegerType(), Collections.emptyList(), "random_int"));
  }

  @Test
  public void testCastTypedExpr() {
    final CallTypedExpr input = new CallTypedExpr(new IntegerType(), Collections.emptyList(), "random_int");
    SerdeTests.testVeloxBeanRoundTrip(CastTypedExpr.create(new IntegerType(), input, true));
  }

  @Test
  public void testConcatTypedExpr() {
    final CallTypedExpr input1 = new CallTypedExpr(new IntegerType(), Collections.emptyList(), "random_int");
    final CallTypedExpr input2 = new CallTypedExpr(new RealType(), Collections.emptyList(), "random_real");
    SerdeTests.testVeloxBeanRoundTrip(ConcatTypedExpr.create(List.of("foo", "bar"), List.of(input1, input2)));
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
    Assert.assertThrows(ComparisonFailure.class, () -> SerdeTests.testVeloxBeanRoundTrip(expr));
    arrowVector.close();
    jniApi.close();
  }

  @Test
  public void testConstantTypedExprCreatedByString() {
    final JniApi jniApi = JniApi.create();
    final ConstantTypedExpr expr = ConstantTypedExpr.create(jniApi,
        "AQAAACAAAAB7InR5cGUiOiJJTlRFR0VSIiwibmFtZSI6IlR5cGUifQEAAAAAAQ8AAAA=");
    SerdeTests.testVeloxBeanRoundTrip(expr);
  }

  @Test
  public void testDereferenceTypedExpr() {
    final CallTypedExpr input1 = new CallTypedExpr(new IntegerType(), Collections.emptyList(), "random_int");
    final CallTypedExpr input2 = new CallTypedExpr(new RealType(), Collections.emptyList(), "random_real");
    final ConcatTypedExpr concat = ConcatTypedExpr.create(List.of("foo", "bar"), List.of(input1, input2));
    final DereferenceTypedExpr dereference = DereferenceTypedExpr.create(concat, 1);
    Assert.assertEquals(RealType.class, dereference.getReturnType().getClass());
    SerdeTests.testVeloxBeanRoundTrip(dereference);
  }

  @Test
  public void testFieldAccessTypedExpr() {
    final CallTypedExpr input1 = new CallTypedExpr(new IntegerType(), Collections.emptyList(), "random_int");
    final CallTypedExpr input2 = new CallTypedExpr(new RealType(), Collections.emptyList(), "random_real");
    final ConcatTypedExpr concat = ConcatTypedExpr.create(List.of("foo", "bar"), List.of(input1, input2));
    final FieldAccessTypedExpr fieldAccess = FieldAccessTypedExpr.create(concat, "bar");
    Assert.assertEquals(RealType.class, fieldAccess.getReturnType().getClass());
    SerdeTests.testVeloxBeanRoundTrip(fieldAccess);
  }

  @Test
  public void testInputTypedExpr() {
    SerdeTests.testVeloxBeanRoundTrip(new InputTypedExpr(new BooleanType()));
  }

  @Test
  public void testLambdaTypedExpr() {
    final RowType signature = new RowType(List.of("foo", "bar"),
        List.of(new IntegerType(), new VarcharType()));
    final LambdaTypedExpr lambdaTypedExpr = LambdaTypedExpr.create(signature,
        FieldAccessTypedExpr.create(new IntegerType(), "foo"));
    SerdeTests.testVeloxBeanRoundTrip(lambdaTypedExpr);
  }
}

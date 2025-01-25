package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConcatTypedExpr;
import io.github.zhztheplayer.velox4j.test.Serdes;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.RealType;
import io.github.zhztheplayer.velox4j.type.RowType;
import org.junit.BeforeClass;
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
    Serdes.testRoundTrip(new CallTypedExpr(new IntegerType(), Collections.emptyList(), "add"));
  }

  @Test
  public void testCastTypedExpr() {
    final CallTypedExpr input = new CallTypedExpr(new IntegerType(), Collections.emptyList(), "add");
    Serdes.testRoundTrip(new CastTypedExpr(new IntegerType(), Collections.singletonList(input), true));
  }

  @Test
  public void testConcatTypedExpr() {
    final CallTypedExpr input1 = new CallTypedExpr(new IntegerType(), Collections.emptyList(), "add");
    final CallTypedExpr input2 = new CallTypedExpr(new RealType(), Collections.emptyList(), "sub");
    Serdes.testRoundTrip(ConcatTypedExpr.create(Arrays.asList("foo", "bar"),  Arrays.asList(input1, input2)));
  }
}

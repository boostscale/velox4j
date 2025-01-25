package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConcatTypedExpr;
import io.github.zhztheplayer.velox4j.test.Serdes;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import org.junit.BeforeClass;
import org.junit.Test;

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
    Serdes.testRoundTrip(new ConcatTypedExpr(new IntegerType(), Collections.emptyList()));
  }
}

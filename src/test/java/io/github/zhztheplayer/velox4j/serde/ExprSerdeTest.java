package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.test.Serdes;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExprSerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.initialize();
  }

  @Test
  public void testCallTypeExpr() {
    Serdes.testRoundTrip(new CallTypedExpr(new IntegerType(), "add", null));
  }
}

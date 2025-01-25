package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.test.Serdes;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import org.junit.BeforeClass;
import org.junit.Test;

public class TypeSerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.initialize();
  }

  @Test
  public void testBoolean() {
    Serdes.testRoundTrip(new BooleanType());
  }

}

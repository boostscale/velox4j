package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.variant.BooleanValue;
import org.junit.BeforeClass;
import org.junit.Test;

public class VariantSerdeTest {
  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testBooleanValue() {
    SerdeTests.testVariantRoundTrip(new BooleanValue(true));
  }
}

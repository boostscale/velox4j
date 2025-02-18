package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.filter.AlwaysTrue;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import org.junit.BeforeClass;
import org.junit.Test;

public class FilterSerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
  }

  @Test
  public void testAlwaysTrue() {
    SerdeTests.testVeloxSerializableRoundTrip(new AlwaysTrue());
  }
}

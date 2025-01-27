package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.filter.AlwaysTrue;
import org.junit.BeforeClass;
import org.junit.Test;

public class FilterSerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testAlwaysTrue() {
    SerdeTests.testVeloxSerializableRoundTrip(new AlwaysTrue());
  }
}

package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.test.Resources;
import org.junit.BeforeClass;
import org.junit.Test;

public class QuerySerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testJsonQuery() {
    final String queryJson = Resources.readResourceAsString("query/example-1.json");
    SerdeTests.testVeloxBeanRoundTrip(queryJson);
  }
}

package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.test.Resources;
import io.github.zhztheplayer.velox4j.test.Serdes;
import org.junit.BeforeClass;
import org.junit.Test;

public class QuerySerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.initialize();
  }

  @Test
  public void testQuery() {
    final String queryJson = Resources.readResourceAsString("query/example-1.json");
    Serdes.testRoundTrip(queryJson);
  }
}

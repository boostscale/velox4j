package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.test.ResourceTests;
import org.junit.BeforeClass;
import org.junit.Test;

public class QuerySerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testReadPlanJsonFromFile() {
    final String queryJson = ResourceTests.readResourceAsString("query/example-1.json");
    SerdeTests.testVeloxSerializableRoundTrip(queryJson, Query.class);
  }
}

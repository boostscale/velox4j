package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.test.ResourceTests;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import org.junit.BeforeClass;
import org.junit.Test;

public class QuerySerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4jTests.ensureInitialized();
  }

  @Test
  public void testReadPlanJsonFromFile() {
    final String queryJson = ResourceTests.readResourceAsString("query/example-1.json");
    SerdeTests.testVeloxSerializableRoundTrip(queryJson, Query.class);
  }
}

package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.sort.SortOrder;
import io.github.zhztheplayer.velox4j.test.Resources;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class QuerySerdeTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testSortOrder() {
    final SortOrder order = new SortOrder(true, true);
    SerdeTests.testJavaBeanRoundTrip(order);
  }

  @Test
  public void testAggregate() {
    final Aggregate aggregate = new Aggregate(
        "sum", List.of(), null, null, null, false);
  }

  @Test
  public void testReadPlanJsonFromFile() {
    final String queryJson = Resources.readResourceAsString("query/example-1.json");
    SerdeTests.testVeloxBeanRoundTrip(queryJson);
  }
}

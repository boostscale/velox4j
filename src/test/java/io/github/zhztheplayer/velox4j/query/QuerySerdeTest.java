package io.github.zhztheplayer.velox4j.query;

import io.github.zhztheplayer.velox4j.test.Resources;
import io.github.zhztheplayer.velox4j.test.Serdes;
import org.junit.Test;

public class QuerySerdeTest {
  public static final String QUERY_PATH = "query/example-1.json";
  public static final String QUERY_OUTPUT_PATH = "query-output/example-1.tsv";

  @Test
  public void testQuery() {
    final String queryJson = readQueryJson();
    Serdes.testRoundTrip(queryJson);
  }

  private static String readQueryJson() {
    return Resources.readResourceAsString(QUERY_PATH);
  }
}

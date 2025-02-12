package io.github.zhztheplayer.velox4j.test;

import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;

import java.util.List;

public final class SampleQueryTests {
  private static final String SAMPLE_QUERY_PATH = "query/example-1.json";
  private static final String SAMPLE_QUERY_OUTPUT_PATH = "query-output/example-1.tsv";
  private static final RowType SAMPLE_QUERY_TYPE = new RowType(List.of("c0", "a0", "a1"), List.of(new BigIntType(), new BigIntType(), new BigIntType()));

  public static RowType getSchema() {
    return SAMPLE_QUERY_TYPE;
  }

  public static String readQueryJson() {
    return ResourceTests.readResourceAsString(SAMPLE_QUERY_PATH);
  }

  public static void assertIterator(UpIterator itr) {
    UpIteratorTests.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0, ResourceTests.readResourceAsString(SAMPLE_QUERY_OUTPUT_PATH))
        .run();
  }
}

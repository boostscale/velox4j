package io.github.zhztheplayer.velox4j.test;

import io.github.zhztheplayer.velox4j.collection.Streams;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;
import org.junit.Assert;

import java.util.List;
import java.util.stream.Collectors;

public final class SampleQueryTests {
  private static final String SAMPLE_QUERY_PATH = "query/example-1.json";
  private static final String SAMPLE_QUERY_OUTPUT_PATH = "query-output/example-1.tsv";
  private static final RowType SAMPLE_QUERY_TYPE = new RowType(List.of("c0", "a0", "a1"), List.of(new BigIntType(), new BigIntType(), new BigIntType()));

  public static RowType getSchema() {
    return SAMPLE_QUERY_TYPE;
  }

  public static String readQueryJson() {
    return Resources.readResourceAsString(SAMPLE_QUERY_PATH);
  }

  public static void assertIterator(UpIterator itr) {
    Iterators.assertIterator(itr)
        .assertNumRowVectors(1)
        .assertRowVectorToString(0, Resources.readResourceAsString(SAMPLE_QUERY_OUTPUT_PATH))
        .run();
  }
}

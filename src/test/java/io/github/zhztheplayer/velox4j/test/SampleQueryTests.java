package io.github.zhztheplayer.velox4j.test;

import io.github.zhztheplayer.velox4j.collection.Streams;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;

import java.util.List;
import java.util.stream.Collectors;

public final class SampleQueryTests {
  private static final String SAMPLE_QUERY_PATH = "query/example-1.json";
  private static final String SAMPLE_QUERY_OUTPUT_PATH = "query-output/example-1.tsv";



  public static String readQueryJson() {
    return Resources.readResourceAsString(SAMPLE_QUERY_PATH);
  }

  public static void assertIterator(UpIterator itr) {
    final RowVector vector = collectSingleVector(itr);
    Assert.assertEquals(Resources.readResourceAsString(SAMPLE_QUERY_OUTPUT_PATH),
        RowVectors.toString(new RootAllocator(), vector));
    vector.close();
    itr.close();
  }

  public static RowVector collectSingleVector(UpIterator itr) {
    final List<RowVector> vectors = collect(itr);
    Assert.assertEquals(1, vectors.size());
    return vectors.get(0);
  }

  private static List<RowVector> collect(UpIterator itr) {
    final List<RowVector> vectors = Streams.fromIterator(itr).collect(Collectors.toList());
    return vectors;
  }
}

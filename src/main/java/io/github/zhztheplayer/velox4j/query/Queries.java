package io.github.zhztheplayer.velox4j.query;

import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.jni.JniApi;

public class Queries {
  private final JniApi jniApi;

  public Queries(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public SerialTask execute(Query query) {
    try (final QueryExecutor exec = jniApi.createQueryExecutor(query)) {
      return exec.execute();
    }
  }

  public QueryExecutor createQueryExecutor(Query query) {
    return jniApi.createQueryExecutor(query);
  }
}

package io.github.zhztheplayer.velox4j.query;

import io.github.zhztheplayer.velox4j.connector.ExternalStream;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.serde.Serde;

public class QueryExecutor implements AutoCloseable {
  private final JniApi jniApi;

  public QueryExecutor(MemoryManager memoryManager) {
    this.jniApi = JniApi.create(memoryManager);
  }

  public UpIterator execute(Query query) {
    final String queryJson = Serde.toPrettyJson(query);
    return jniApi.executeQuery(queryJson);
  }

  public UpIterator execute(String queryJson) {
    return jniApi.executeQuery(queryJson);
  }

  public ExternalStream bindDownIterator(DownIterator down) {
    return jniApi.downIteratorAsExternalStream(down);
  }

  @Override
  public void close() {
    jniApi.close();
  }
}

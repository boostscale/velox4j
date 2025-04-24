package io.github.zhztheplayer.velox4j.query;

import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.jni.CppObject;
import io.github.zhztheplayer.velox4j.jni.JniApi;

public class QueryExecutor implements CppObject {
  private final JniApi jniApi;
  private final long id;

  public QueryExecutor(JniApi jniApi, long id) {
    this.jniApi = jniApi;
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  public SerialTask execute() {
    return jniApi.queryExecutorExecute(this);
  }
}

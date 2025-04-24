package io.github.zhztheplayer.velox4j.query;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;

public class SerialTask implements UpIterator {
  private final JniApi jniApi;
  private final long id;

  public SerialTask(JniApi jniApi, long id) {
    this.jniApi = jniApi;
    this.id = id;
  }

  @Override
  public State advance() {
    return StaticJniApi.get().upIteratorAdvance(this);
  }

  @Override
  public void waitFor() {
    StaticJniApi.get().upIteratorWait(this);
  }

  @Override
  public RowVector get() {
    return jniApi.upIteratorGet(this);
  }

  @Override
  public long id() {
    return id;
  }

  public QueryStats collectStats() {
    return StaticJniApi.get().serialTaskCollectStats(this);
  }
}

package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.lifecycle.CppObject;

import java.util.Iterator;

public class UpIterator implements CppObject, Iterator<RowVector> {
  private final JniApi jniApi;
  private final long id;

  public UpIterator(JniApi jniApi, long id) {
    this.jniApi = jniApi;
    this.id = id;
  }

  @Override
  public boolean hasNext() {
    return jniApi.upIteratorHasNext(this);
  }

  @Override
  public RowVector next() {
    return jniApi.upIteratorNext(this);
  }

  @Override
  public JniApi jniApi() {
    return jniApi;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public void close() {
    jniApi.releaseCppObject(this);
  }
}

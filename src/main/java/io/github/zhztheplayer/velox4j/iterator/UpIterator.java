package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.lifecycle.CppObject;

import java.util.Iterator;

public class UpIterator implements CppObject, Iterator<RowVector> {
  private final long address;

  public UpIterator(long address) {
    this.address = address;
  }

  @Override
  public boolean hasNext() {
    return JniApi.upIteratorHasNext(this);
  }

  @Override
  public RowVector next() {
    return JniApi.upIteratorNext(this);
  }

  @Override
  public long address() {
    return address;
  }

  @Override
  public void close() {
    JniApi.closeCppObject(this);
  }
}

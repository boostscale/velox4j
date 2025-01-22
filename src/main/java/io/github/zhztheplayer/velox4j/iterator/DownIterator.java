package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.data.RowVector;

import java.util.Iterator;

public class DownIterator {
  private final Iterator<RowVector> delegated;

  public DownIterator(Iterator<RowVector> delegated) {
    this.delegated = delegated;
  }

  public boolean hasNext() {
    return delegated.hasNext();
  }

  public long next() {
    return delegated.next().address();
  }
}

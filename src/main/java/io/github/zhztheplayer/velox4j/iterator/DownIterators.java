package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.data.RowVector;

import java.util.Iterator;

public final class DownIterators {
  public static DownIterator fromJavaIterator(Iterator<RowVector> itr) {
    return new FromJavaIterator(itr);
  }

  private static class FromJavaIterator extends BaseDownIterator {
    private final Iterator<RowVector> itr;

    private FromJavaIterator(Iterator<RowVector> itr) {
      this.itr = itr;
    }

    @Override
    public State advance0() {
      if (!itr.hasNext()) {
        return State.FINISHED;
      }
      return State.AVAILABLE;
    }

    @Override
    public void waitFor() throws InterruptedException {
      throw new IllegalStateException("#waitFor is called while the iterator doesn't block");
    }

    @Override
    public RowVector get0() {
      return itr.next();
    }

    @Override
    public void close() {

    }
  }

  private static abstract class BaseDownIterator implements DownIterator {
    protected BaseDownIterator() {
    }

    @Override
    public final int advance() {
      return advance0().getId();
    }

    @Override
    public final long get() {
      return get0().id();
    }

    protected abstract DownIterator.State advance0();
    protected abstract RowVector get0();
  }
}

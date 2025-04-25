package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.exception.VeloxException;

public final class UpIterators {
  public static CloseableIterator<RowVector> asJavaIterator(UpIterator upIterator) {
    return new AsJavaIterator(upIterator);
  }

  public static InfiniteIterator<RowVector> asInfiniteIterator(UpIterator upIterator) {
    return new AsInfiniteIterator(upIterator);
  }

  private static class AsJavaIterator implements CloseableIterator<RowVector> {
    private final UpIterator upIterator;

    private AsJavaIterator(UpIterator upIterator) {
      this.upIterator = upIterator;
    }

    @Override
    public boolean hasNext() {
      while (true) {
        final UpIterator.State state = upIterator.advance();
        switch (state) {
          case BLOCKED:
            upIterator.waitFor();
            continue;
          case AVAILABLE:
            return true;
          case FINISHED:
            return false;
        }
      }
    }

    @Override
    public RowVector next() {
      return upIterator.get();
    }

    @Override
    public void close() throws Exception {
      upIterator.close();
    }
  }

  private static class AsInfiniteIterator implements InfiniteIterator<RowVector> {
    private final UpIterator upIterator;
    private boolean isAvailable = false;

    private AsInfiniteIterator(UpIterator upIterator) {
      this.upIterator = upIterator;
    }

    @Override
    public boolean available() {
      if (isAvailable) {
        return true;
      }
      final UpIterator.State state = upIterator.advance();
      switch (state) {
        case BLOCKED:
          return false;
        case AVAILABLE:
          isAvailable = true;
          return true;
        case FINISHED:
          throw new VeloxException("InfiniteIterator was closed by user");
        default:
          throw new IllegalStateException("Unknown state: " + state);
      }
    }

    @Override
    public void waitFor() {
      if (isAvailable) {
        return;
      }
      final UpIterator.State state = upIterator.advance();
      switch (state) {
        case BLOCKED:
          upIterator.waitFor();
          return;
        case AVAILABLE:
          isAvailable = true;
          return;
        case FINISHED:
          throw new VeloxException("InfiniteIterator was closed by user");
        default:
          throw new IllegalStateException("Unknown state: " + state);
      }
    }

    @Override
    public RowVector get() {
      if (!isAvailable) {
        throw new VeloxException("AsInfiniteIterator#get can only be called after #available() returns true");
      }
      final RowVector rv = upIterator.get();
      isAvailable = false;
      return rv;
    }

    @Override
    public void close() throws Exception {
      upIterator.close();
    }
  }
}

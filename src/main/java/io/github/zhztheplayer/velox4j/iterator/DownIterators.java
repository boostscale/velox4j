package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.data.RowVector;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DownIterators {
  public static DownIterator fromJavaIterator(Iterator<RowVector> itr) {
    return new FromJavaIterator(itr);
  }

  // Deprecated: Use ExternalStreams.newBlockingQueue() instead.
  @Deprecated
  public static DownIterator fromBlockingQueue(BlockingQueue<RowVector> queue) {
    return new FromBlockingQueue(queue);
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

  private static class FromBlockingQueue extends BaseDownIterator {
    private final BlockingQueue<RowVector> queue;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private RowVector pending = null;

    public FromBlockingQueue(BlockingQueue<RowVector> queue) {
      this.queue = queue;
    }

    @Override
    public State advance0() {
      if (pending != null) {
        return State.AVAILABLE;
      }
      if (queue.isEmpty()) {
        return State.BLOCKED;
      }
      return State.AVAILABLE;
    }

    @Override
    public void waitFor() throws InterruptedException {
      while (true) {
        if (pending != null) {
          return;
        }
        if (closed.get()) {
          Thread.currentThread().interrupt();
          throw new InterruptedException();
        }
        pending = queue.poll(100L, TimeUnit.MILLISECONDS);
      }
    }

    @Override
    public RowVector get0() {
      if (pending != null) {
        final RowVector out = pending;
        pending = null;
        return out;
      }
      return queue.remove();
    }

    @Override
    public void close() {
      closed.compareAndSet(false, true);
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

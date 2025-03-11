package io.github.zhztheplayer.velox4j.iterator;

import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.data.RowVector;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DownIterators {
  public static DownIterator fromJavaIterator(Iterator<RowVector> itr) {
    return new FromJavaIterator(itr);
  }

  public static DownIterator fromBlockingQueue(BlockingQueue<RowVector> queue) {
    return new FromBlockingQueue(queue);
  }

  private static class FromJavaIterator implements DownIterator {
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
    public long get() {
      return itr.next().id();
    }

    @Override
    public void close() {

    }
  }

  private static class FromBlockingQueue implements DownIterator {
    private final BlockingQueue<RowVector> queue;
    private RowVector pending = null;
    private AtomicBoolean closed = new AtomicBoolean(false);

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
    public long get() {
      if (pending != null) {
        final RowVector out = pending;
        pending = null;
        return out.id();
      }
      return queue.remove().id();
    }

    @Override
    public void close() {
      closed.compareAndSet(false, true);
    }
  }
}

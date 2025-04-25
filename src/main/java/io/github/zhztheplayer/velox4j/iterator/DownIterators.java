package io.github.zhztheplayer.velox4j.iterator;

import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.exception.VeloxException;

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

    @Override
    public void finish() {
      throw new VeloxException("#finish is not implemented");
    }
  }

  private static class FromBlockingQueue implements DownIterator {
    private final BlockingQueue<RowVector> queue;
    private RowVector pending = null;
    private RowVector prevRowVector = null;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private AtomicBoolean finished = new AtomicBoolean(false);

    public FromBlockingQueue(BlockingQueue<RowVector> queue) {
      this.queue = queue;
    }

    @Override
    public State advance0() {
      /*
       * DownIterator owns the RowVector and is responsible for releasing its resources.
       * It's safe to release previous RowVector here.
       */
      if (prevRowVector != null) {
        prevRowVector.close();
        prevRowVector = null;
      }
      if (pending != null) {
        return State.AVAILABLE;
      }
      if (finished.get() && queue.isEmpty()) {
        return State.FINISHED;
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
        if (pending == null && finished.get()) {
          // If the queue is empty and finished, we should return
          // to avoid blocking forever.
          return;
        }
      }
    }

    @Override
    public long get() {
      if (pending != null) {
        final RowVector out = pending;
        prevRowVector = pending;
        pending = null;
        return out.id();
      }
      prevRowVector = queue.remove();
      return prevRowVector.id();
      // return queue.remove().id();
    }

    @Override
    public void close() {
      closed.compareAndSet(false, true);
    }

    @Override
    public void finish() {
      finished.compareAndSet(false, true);
    }
  }
}

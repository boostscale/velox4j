package io.github.zhztheplayer.velox4j.test;

import io.github.zhztheplayer.velox4j.collection.Streams;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class UpIteratorTests {
  public static RowVector collectSingleVector(UpIterator itr) {
    final List<RowVector> vectors = collect(itr);
    Assert.assertEquals(1, vectors.size());
    return vectors.get(0);
  }

  private static List<RowVector> collect(UpIterator itr) {
    final List<RowVector> vectors = Streams.fromIterator(itr).collect(Collectors.toList());
    return vectors;
  }

  public static IteratorAssertionBuilder assertIterator(UpIterator itr) {
    return new IteratorAssertionBuilder(itr);
  }

  public static class IteratorAssertionBuilder {
    private final RootAllocator alloc = new RootAllocator();
    private final UpIterator itr;
    private final List<Consumer<Argument>> assertions = new ArrayList<>();
    private final List<Runnable> finalAssertions = new ArrayList<>();

    private IteratorAssertionBuilder(UpIterator itr) {
      this.itr = itr;
    }

    public IteratorAssertionBuilder assertNumRowVectors(int expected) {
      final AtomicInteger count = new AtomicInteger();
      assertForEach(new Consumer<Argument>() {
        @Override
        public void accept(Argument argument) {
          count.getAndIncrement();
        }
      });
      assertFinal(new Runnable() {
        @Override
        public void run() {
          Assert.assertEquals(expected, count.get());
        }
      });
      return this;
    }

    public IteratorAssertionBuilder assertRowVectorToString(int i, String expected) {
      return assertRowVector(i, new Consumer<RowVector>() {
        @Override
        public void accept(RowVector vector) {
          Assert.assertEquals(expected, RowVectors.toString(alloc, vector));
        }
      });
    }

    public IteratorAssertionBuilder assertRowVector(int i, Consumer<RowVector> body) {
      assertForEach(new Consumer<Argument>() {
        @Override
        public void accept(Argument argument) {
          if (argument.i == i) {
            body.accept(argument.rv);
          }
        }
      });
      return this;
    }

    private void assertForEach(Consumer<Argument> body) {
      assertions.add(body);
    }

    private void assertFinal(Runnable body) {
      finalAssertions.add(body);
    }

    public void run() {
      int i = 0;
      while (itr.hasNext()) {
        final RowVector rv = itr.next();
        for (Consumer<Argument> assertion : assertions) {
          assertion.accept(new Argument(i, rv));
        }
        i++;
      }
      for (Runnable r : finalAssertions) {
        r.run();
      }
      alloc.close();
    }

    private static class Argument {
      private final int i;
      private final RowVector rv;

      private Argument(int i, RowVector rv) {
        this.i = i;
        this.rv = rv;
      }
    }
  }
}

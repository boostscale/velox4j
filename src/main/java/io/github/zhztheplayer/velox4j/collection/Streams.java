package io.github.zhztheplayer.velox4j.collection;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class Streams {
  public static <T> Stream<T> fromIterator(Iterator<T> itr) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(itr, Spliterator.ORDERED), false);
  }

  public static <T> Stream<T> fromIterable(Iterable<T> itr) {
    return fromIterator(itr.iterator());
  }
}

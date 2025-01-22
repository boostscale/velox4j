package io.github.zhztheplayer.velox4j.test;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class Iterators {
  public static <T> Stream<T> asStream(Iterator<T> itr) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(itr, Spliterator.ORDERED), false);
  }
}

package io.github.zhztheplayer.velox4j.iterator;

import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {
    /*
     * Prepares the next element in the iterator.
     *
     * @param blocking If true, the method will block until the next element is available.
     *                 If false, the method will return immediately if no element is available.
     * @return true if the next element is prepared successfully, false otherwise.
     */
    boolean hasNext(boolean blocking);
}

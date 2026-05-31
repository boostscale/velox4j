/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.boostscale.velox4j.iterator;

import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.exception.VeloxException;

public final class ExportIterators {
  public static CloseableIterator<RowVector> asJavaIterator(ExportIterator exportIterator) {
    return new AsJavaIterator(exportIterator);
  }

  public static InfiniteIterator<RowVector> asInfiniteIterator(ExportIterator exportIterator) {
    return new AsInfiniteIterator(exportIterator);
  }

  private static class AsJavaIterator implements CloseableIterator<RowVector> {
    private final ExportIterator exportIterator;

    private AsJavaIterator(ExportIterator exportIterator) {
      this.exportIterator = exportIterator;
    }

    @Override
    public boolean hasNext() {
      while (true) {
        final ExportIterator.State state = exportIterator.advance();
        switch (state) {
          case BLOCKED:
            exportIterator.waitFor();
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
      return exportIterator.get();
    }

    @Override
    public void close() throws Exception {
      exportIterator.close();
    }
  }

  private static class AsInfiniteIterator implements InfiniteIterator<RowVector> {
    private final ExportIterator exportIterator;
    private boolean isAvailable = false;

    private AsInfiniteIterator(ExportIterator exportIterator) {
      this.exportIterator = exportIterator;
    }

    @Override
    public boolean available() {
      if (isAvailable) {
        return true;
      }
      final ExportIterator.State state = exportIterator.advance();
      switch (state) {
        case BLOCKED:
          return false;
        case AVAILABLE:
          isAvailable = true;
          return true;
        case FINISHED:
          throw new VeloxException(
              "InfiniteIterator reaches FINISHED state, which is not supposed to happen");
        default:
          throw new IllegalStateException("Unknown state: " + state);
      }
    }

    @Override
    public void waitFor() {
      if (isAvailable) {
        return;
      }
      final ExportIterator.State state = exportIterator.advance();
      switch (state) {
        case BLOCKED:
          exportIterator.waitFor();
          return;
        case AVAILABLE:
          isAvailable = true;
          return;
        case FINISHED:
          throw new VeloxException(
              "InfiniteIterator reaches FINISHED state, which is not supposed to happen");
        default:
          throw new IllegalStateException("Unknown state: " + state);
      }
    }

    @Override
    public RowVector get() {
      if (!isAvailable) {
        throw new VeloxException(
            "AsInfiniteIterator#get can only be called after #available() returns true");
      }
      final RowVector rv = exportIterator.get();
      isAvailable = false;
      return rv;
    }

    @Override
    public void close() throws Exception {
      exportIterator.close();
    }
  }
}

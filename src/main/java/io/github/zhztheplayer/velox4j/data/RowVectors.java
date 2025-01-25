package io.github.zhztheplayer.velox4j.data;

import io.github.zhztheplayer.velox4j.arrow.Arrow;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.table.Table;

public final class RowVectors {
  private RowVectors() {

  }

  public static String toString(BufferAllocator alloc, RowVector rv) {
    try (final Table t = Arrow.toArrowTable(alloc, rv); final VectorSchemaRoot vsr = t.toVectorSchemaRoot()) {
      return vsr.contentToTSVString();
    }
  }
}

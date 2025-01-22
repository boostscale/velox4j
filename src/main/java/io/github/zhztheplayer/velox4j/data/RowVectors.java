package io.github.zhztheplayer.velox4j.data;

import io.github.zhztheplayer.velox4j.jni.JniApi;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.table.Table;

public final class RowVectors {
  Table toArrowTable(BufferAllocator alloc, RowVector vector) {
    final ArrowSchema schema = ArrowSchema.allocateNew(alloc);
    final ArrowArray array = ArrowArray.allocateNew(alloc);
    try {
      JniApi.rowVectorExportToArrow(vector, schema, array);
      final VectorSchemaRoot vsr = Data.importVectorSchemaRoot(alloc, array, schema, null);
      return new Table(vsr);
    } finally {
      schema.release();
      schema.close();
      array.release();
      array.close();
    }
  }
}

package io.github.zhztheplayer.velox4j.arrow;

import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.jni.Session;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.table.Table;

public class Arrow {
  private Arrow() {

  }

  public static Table toArrowTable(BufferAllocator alloc, RowVector vector) {
    try (final ArrowSchema schema = ArrowSchema.allocateNew(alloc);
        final ArrowArray array = ArrowArray.allocateNew(alloc)) {
      vector.jniApi().baseVectorToArrow(vector, schema, array);
      final VectorSchemaRoot vsr = Data.importVectorSchemaRoot(alloc, array, schema, null);
      return new Table(vsr);
    }
  }

  public static FieldVector toArrowVector(BufferAllocator alloc, RowVector vector) {
    try (final ArrowSchema schema = ArrowSchema.allocateNew(alloc);
        final ArrowArray array = ArrowArray.allocateNew(alloc)) {
      vector.jniApi().baseVectorToArrow(vector, schema, array);
      final FieldVector fv = Data.importVector(alloc, array, schema, null);
      return fv;
    }
  }

  public static BaseVector fromArrowVector(Session session, BufferAllocator alloc, FieldVector arrowVector) {
    try (final ArrowSchema cSchema1 = ArrowSchema.allocateNew(alloc);
        final ArrowArray cArray1 = ArrowArray.allocateNew(alloc)) {
      Data.exportVector(alloc, arrowVector, null, cArray1, cSchema1);
      final BaseVector imported = session.arrowToBaseVector(cSchema1, cArray1);
      return imported;
    }
  }
}

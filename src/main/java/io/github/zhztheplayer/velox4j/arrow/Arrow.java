package io.github.zhztheplayer.velox4j.arrow;

import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.table.Table;

public class Arrow {
  private final JniApi jniApi;

  public Arrow(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public static FieldVector toArrowVector(BufferAllocator alloc, BaseVector vector) {
    try (final ArrowSchema cSchema = ArrowSchema.allocateNew(alloc);
        final ArrowArray cArray = ArrowArray.allocateNew(alloc)) {
      StaticJniApi.get().baseVectorToArrow(vector, cSchema, cArray);
      final FieldVector fv = Data.importVector(alloc, cArray, cSchema, null);
      return fv;
    }
  }

  public static Table toArrowTable(BufferAllocator alloc, RowVector vector) {
    try (final ArrowSchema cSchema = ArrowSchema.allocateNew(alloc);
        final ArrowArray cArray = ArrowArray.allocateNew(alloc)) {
      StaticJniApi.get().baseVectorToArrow(vector, cSchema, cArray);
      final VectorSchemaRoot vsr = Data.importVectorSchemaRoot(alloc, cArray, cSchema, null);
      return new Table(vsr);
    }
  }

  public BaseVector fromArrowVector(BufferAllocator alloc, FieldVector arrowVector) {
    try (final ArrowSchema cSchema = ArrowSchema.allocateNew(alloc);
        final ArrowArray cArray = ArrowArray.allocateNew(alloc)) {
      Data.exportVector(alloc, arrowVector, null, cArray, cSchema);
      final BaseVector imported = jniApi.arrowToBaseVector(cSchema, cArray);
      return imported;
    }
  }

  public RowVector fromArrowTable(BufferAllocator alloc, Table table) {
    try (final ArrowSchema cSchema = ArrowSchema.allocateNew(alloc);
        final ArrowArray cArray = ArrowArray.allocateNew(alloc);
        final VectorSchemaRoot vsr = table.toVectorSchemaRoot()) {
      Data.exportVectorSchemaRoot(alloc, vsr, null, cArray, cSchema);
      final BaseVector imported = jniApi.arrowToBaseVector(cSchema, cArray);
      return imported.asRowVector();
    }
  }
}

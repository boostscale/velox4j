package io.github.zhztheplayer.velox4j.data;

import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.table.Table;

public class RowVectors {
  private final JniApi jniApi;

  public RowVectors(JniApi jniApi) {
    this.jniApi = jniApi;
  }
}

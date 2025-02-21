package io.github.zhztheplayer.velox4j.data;

import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.jni.CppObject;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;
import io.github.zhztheplayer.velox4j.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;

public class BaseVector implements CppObject {
  public static BaseVector wrap(JniApi jniApi, long id, VectorEncoding encoding) {
    if (encoding == VectorEncoding.ROW) {
      return new RowVector(jniApi, id);
    }
    return new BaseVector(jniApi, id);
  }

  private final JniApi jniApi;
  private final long id;

  protected BaseVector(JniApi jniApi, long id) {
    this.jniApi = jniApi;
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  public Type getType() {
    return StaticJniApi.get().baseVectorGetType(this);
  }

  public VectorEncoding getEncoding() {
    return StaticJniApi.get().baseVectorGetEncoding(this);
  }

  public int getSize() {
    return StaticJniApi.get().baseVectorGetSize(this);
  }

  public BaseVector wrapInConstant(int length, int index) {
    return jniApi.baseVectorWrapInConstant(this, length, index);
  }

  public RowVector asRowVector() {
    if (this instanceof RowVector) {
      return (RowVector) this;
    }
    throw new VeloxException(String.format("Not a RowVector. Encoding: %s", getEncoding()));
  }

  public String serialize() {
    return BaseVectors.serializeOne(this);
  }

  public String toString(BufferAllocator alloc) {
    try (final FieldVector fv = Arrow.toArrowVector(alloc, this)) {
      return fv.toString();
    }
  }

  @Override
  public String toString() {
    try (final BufferAllocator alloc = new RootAllocator()) {
      return toString(alloc);
    }
  }
}

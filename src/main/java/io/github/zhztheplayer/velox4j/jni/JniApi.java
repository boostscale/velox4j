package io.github.zhztheplayer.velox4j.jni;

import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.VectorEncoding;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.lifecycle.CppObject;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.type.Type;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

/**
 * The higher-level JNI-based API than {@link JniWrapper}. The API hides C++ pointers from
 * developers with providing objective representations in Java to caller.
 */
public final class JniApi implements CppObject {
  public static JniApi create() {
    return new JniApi();
  }

  private final JniWrapper jni = JniWrapper.create();

  private JniApi() {
  }

  public UpIterator executeQuery(String jsonQuery) {
    return new UpIterator(this, jni.executeQuery(jsonQuery));
  }

  public void releaseCppObject(CppObject obj) {
    jni.releaseCppObject(obj.id());
  }

  public boolean upIteratorHasNext(UpIterator itr) {
    return jni.upIteratorHasNext(itr.id());
  }

  public RowVector upIteratorNext(UpIterator itr) {
    return new RowVector(this, jni.upIteratorNext(itr.id()));
  }

  public BaseVector arrowToBaseVector(ArrowSchema schema, ArrowArray array) {
    return new RowVector(this, jni.arrowToBaseVector(schema.memoryAddress(), array.memoryAddress()));
  }

  public void baseVectorToArrow(BaseVector vector, ArrowSchema schema, ArrowArray array) {
    jni.baseVectorToArrow(vector.id(), schema.memoryAddress(), array.memoryAddress());
  }

  public String baseVectorSerialize(BaseVector vector) {
    return jni.baseVectorSerialize(vector.id());
  }

  public BaseVector baseVectorDeserialize(String serialized) {
    return new RowVector(this, jni.baseVectorDeserialize(serialized));
  }

  public Type baseVectorGetType(BaseVector vector) {
    String typeJson = jni.baseVectorGetType(vector.id());
    return (Type) Serde.fromJson(typeJson);
  }

  public BaseVector baseVectorWrapInConstant(BaseVector vector, int length, int index) {
    return new RowVector(this, jni.baseVectorWrapInConstant(vector.id(), length, index));
  }

  public VectorEncoding baseVectorGetEncoding(BaseVector vector) {
    return VectorEncoding.valueOf(jni.baseVectorGetEncoding(vector.id()));
  }

  public String deserializeAndSerialize(String json) {
    return jni.deserializeAndSerialize(json);
  }

  @Override
  public JniApi jniApi() {
    return this;
  }

  @Override
  public long id() {
    return jni.sessionId();
  }
}

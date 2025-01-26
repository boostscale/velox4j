package io.github.zhztheplayer.velox4j.data;

import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.type.Type;

public class BaseVectors {
  private BaseVectors() {
  }

  public static String serialize(BaseVector vector) {
    return vector.jniApi().baseVectorSerialize(vector);
  }

  public static BaseVector deserialize(JniApi jniApi, String serialized) {
    return jniApi.baseVectorDeserialize(serialized);
  }

  public static Type getType(BaseVector vector) {
    return vector.jniApi().baseVectorGetType(vector);
  }

  public static BaseVector wrapInConstant(BaseVector vector, int length, int index) {
    return vector.jniApi().baseVectorWrapInConstant(vector, length, index);
  }

  public static VectorEncoding getEncoding(BaseVector vector) {
    return vector.jniApi().baseVectorGetEncoding(vector);
  }
}

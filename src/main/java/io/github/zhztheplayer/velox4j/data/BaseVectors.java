package io.github.zhztheplayer.velox4j.data;

public class BaseVectors {
  private BaseVectors() {
  }

  public static String serialize(BaseVector vector) {
    return vector.jniApi().baseVectorSerialize(vector);
  }
}

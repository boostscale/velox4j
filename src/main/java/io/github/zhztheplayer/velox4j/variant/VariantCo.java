package io.github.zhztheplayer.velox4j.variant;

import io.github.zhztheplayer.velox4j.jni.CppObject;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;

/**
 * Binds a CPP variant object.
 */
public class VariantCo implements CppObject {
  private final long id;

  public VariantCo(long id) {
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  public Variant asJava() {
    return StaticJniApi.get().variantAsJava(this);
  }
}

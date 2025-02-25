package io.github.zhztheplayer.velox4j.variant;

import io.github.zhztheplayer.velox4j.jni.JniApi;

import java.util.List;

public class Variants {
  private final JniApi jniApi;

  public Variants(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public VariantCo asCpp(Variant variant) {
    return jniApi.variantAsCpp(variant);
  }

  public static void checkSameType(List<Variant> variants) {
    if (variants.size() <= 1) {
      return;
    }
    for (int i = 1; i < variants.size(); i++) {
      if (variants.get(i).getClass() != variants.get(i - 1).getClass()) {
        throw new IllegalArgumentException("All variant values should have same type");
      }
    }
  }
}

package io.github.zhztheplayer.velox4j.test;

import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.bean.VeloxBean;
import org.junit.Assert;

public final class Serdes {
  public static void testRoundTrip(VeloxBean bean) {
    try(final JniApi jniApi = JniApi.create()) {
      final String beforeNative = Serde.toPrettyJson(bean);
      final String afterNative = jniApi.deserializeAndSerialize(beforeNative);
      final VeloxBean deserialized = Serde.fromJson(afterNative);
      final String serialized = Serde.toPrettyJson(deserialized);
      Assert.assertEquals(beforeNative, serialized);
    }
  }

  public static void testRoundTrip(String json) {
    try(final JniApi jniApi = JniApi.create()) {
      final String afterNative = jniApi.deserializeAndSerialize(json);
      Assert.assertEquals(json, afterNative);
    }
  }
}

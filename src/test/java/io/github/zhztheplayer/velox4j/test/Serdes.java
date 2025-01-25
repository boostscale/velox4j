package io.github.zhztheplayer.velox4j.test;

import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.serde.JsonSerde;
import io.github.zhztheplayer.velox4j.serde.VeloxBean;
import org.junit.Assert;

public final class Serdes {
  public static void testRoundTrip(VeloxBean bean) {
    try(final JniApi jniApi = JniApi.create()) {
      final String beforeNative = JsonSerde.toPrettyJson(bean);
      final String afterNative = jniApi.deserializeAndSerialize(beforeNative);
      final VeloxBean deserialized = JsonSerde.fromJson(afterNative);
      final String serialized = JsonSerde.toPrettyJson(deserialized);
      Assert.assertEquals(serialized, beforeNative);
    }
  }

  public static void testRoundTrip(String json) {
    try(final JniApi jniApi = JniApi.create()) {
      final String afterNative = jniApi.deserializeAndSerialize(json);
      Assert.assertEquals(afterNative, json);
    }
  }
}

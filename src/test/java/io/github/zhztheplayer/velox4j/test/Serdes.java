package io.github.zhztheplayer.velox4j.test;

import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.serde.JsonSerde;
import org.junit.Assert;

public final class Serdes {
  public static void testRoundTrip(Object object) {
    try(final JniApi jniApi = JniApi.create()) {
      final String beforeNative = JsonSerde.toPrettyJson(object);
      final String afterNative = jniApi.deserializeAndSerialize(beforeNative);
      final Object deserialized = JsonSerde.fromJson(object.getClass(), afterNative);
      final String reserialized = JsonSerde.toJson(deserialized);
      Assert.assertEquals(beforeNative, reserialized);
    }
  }

  public static void testRoundTrip(String json) {
    try(final JniApi jniApi = JniApi.create()) {
      final String afterNative = jniApi.deserializeAndSerialize(json);
      Assert.assertEquals(json, afterNative);
    }
  }
}

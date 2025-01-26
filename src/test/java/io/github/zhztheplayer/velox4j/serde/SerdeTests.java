package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.bean.VeloxBean;
import org.junit.Assert;

public final class SerdeTests {
  public static String testVeloxBeanRoundTrip(VeloxBean inObj) {
    try(final JniApi jniApi = JniApi.create()) {
      final String inJson = Serde.toPrettyJson(inObj);
      final String outJson = jniApi.deserializeAndSerialize(inJson);
      final VeloxBean outObj = Serde.fromJson(outJson);
      final String outJson2 = Serde.toPrettyJson(outObj);
      Assert.assertEquals(inJson, outJson2);
      return outJson2;
    }
  }

  public static String testVeloxBeanRoundTrip(String inJson) {
    try(final JniApi jniApi = JniApi.create()) {
      final String outJson = jniApi.deserializeAndSerialize(inJson);
      Assert.assertEquals(inJson, outJson);
      return outJson;
    }
  }

  public static String testJavaBeanRoundTrip(Object inObj) {
    try {
      final Class<?> clazz = inObj.getClass();
      final ObjectMapper jsonMapper = Serde.newVeloxJsonMapper();
      final String inJson = jsonMapper.writeValueAsString(inObj);
      final Object outObj = jsonMapper.readValue(inJson, clazz);
      final String outJson = jsonMapper.writeValueAsString(outObj);
      Assert.assertEquals(inJson, outJson);
      return outJson;
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}

package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.cfg.CacheProvider;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.github.zhztheplayer.velox4j.exception.VeloxException;

import java.io.IOException;

public final class JsonSerde {
  private static final ObjectMapper JSON = newVeloxJsonMapper();

  private static ObjectMapper newVeloxJsonMapper() {
    final ObjectMapper jsonMapper = new ObjectMapper();
    return jsonMapper;
  }

  public static String toJson(VeloxBean bean) {
    try {
      return JSON.writer().writeValueAsString(bean);
    } catch (JsonProcessingException e) {
      throw new VeloxException(e);
    }
  }

  public static String toPrettyJson(VeloxBean bean) {
    try {
      return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(bean);
    } catch (JsonProcessingException e) {
      throw new VeloxException(e);
    }
  }

  public static VeloxBean fromJson(String json) {
    try {
      return JSON.reader().readValue(json, VeloxBean.class);
    } catch (IOException e) {
      throw new VeloxException(e);
    }
  }
}

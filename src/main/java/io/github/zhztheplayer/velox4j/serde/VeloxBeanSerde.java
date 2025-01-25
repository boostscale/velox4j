package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.github.zhztheplayer.velox4j.exception.VeloxException;

import java.io.IOException;

public final class VeloxBeanSerde {
  private static final ObjectMapper JSON = newVeloxJsonMapper();

  private static ObjectMapper newVeloxJsonMapper() {
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    jsonMapper.enable(JsonGenerator.Feature.STRICT_DUPLICATE_DETECTION);
    jsonMapper.registerModule(new SimpleModule().setDeserializerModifier(new VeloxBeanDeserializer.Modifier()));
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

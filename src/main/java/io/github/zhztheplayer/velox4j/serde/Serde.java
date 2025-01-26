package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.github.zhztheplayer.velox4j.bean.VeloxBean;
import io.github.zhztheplayer.velox4j.bean.VeloxBeanDeserializer;
import io.github.zhztheplayer.velox4j.bean.VeloxBeanSerializer;
import io.github.zhztheplayer.velox4j.exception.VeloxException;

import java.io.IOException;

public final class Serde {
  private static final ObjectMapper JSON = newVeloxJsonMapper();

  static ObjectMapper newVeloxJsonMapper() {
    final JsonMapper.Builder jsonMapper = JsonMapper.builder();
    jsonMapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    jsonMapper.enable(JsonGenerator.Feature.STRICT_DUPLICATE_DETECTION);
    jsonMapper.disable(MapperFeature.AUTO_DETECT_FIELDS);
    jsonMapper.disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
    jsonMapper.disable(MapperFeature.AUTO_DETECT_GETTERS);
    jsonMapper.disable(MapperFeature.AUTO_DETECT_SETTERS);
    jsonMapper.disable(MapperFeature.AUTO_DETECT_CREATORS);
    jsonMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    jsonMapper.addModule(new SimpleModule().setDeserializerModifier(new VeloxBeanDeserializer.Modifier()));
    jsonMapper.addModule(new SimpleModule().setSerializerModifier(new VeloxBeanSerializer.Modifier()));
    return jsonMapper.build();
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

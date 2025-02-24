package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.zhztheplayer.velox4j.exception.VeloxException;

import java.io.IOException;

public final class Serde {
  private static final ObjectMapper JSON = newVeloxJsonMapper();

  private static ObjectMapper newVeloxJsonMapper() {
    final JsonMapper.Builder jsonMapper = JsonMapper.builder();
    jsonMapper.serializationInclusion(JsonInclude.Include.NON_NULL);
    jsonMapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    jsonMapper.enable(JsonGenerator.Feature.STRICT_DUPLICATE_DETECTION);
    jsonMapper.disable(MapperFeature.AUTO_DETECT_FIELDS);
    jsonMapper.disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
    jsonMapper.disable(MapperFeature.AUTO_DETECT_GETTERS);
    jsonMapper.disable(MapperFeature.AUTO_DETECT_SETTERS);
    jsonMapper.disable(MapperFeature.AUTO_DETECT_CREATORS);
    jsonMapper.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    jsonMapper.disable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES);
    jsonMapper.addModule(new Jdk8Module());
    return jsonMapper.build();
  }

  public static void registerBaseClass(Class<? extends NativeBean> baseClass) {
    JSON.registerModule(new SimpleModule().setDeserializerModifier(new PolymorphicDeserializer.Modifier(baseClass)));
    JSON.registerModule(new SimpleModule().setSerializerModifier(new PolymorphicSerializer.Modifier(baseClass)));
  }

  static ObjectMapper jsonMapper() {
    return JSON;
  }

  public static String toJson(NativeBean bean) {
    try {
      return JSON.writer().writeValueAsString(bean);
    } catch (JsonProcessingException e) {
      throw new VeloxException(e);
    }
  }

  public static String toPrettyJson(NativeBean bean) {
    try {
      return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(bean);
    } catch (JsonProcessingException e) {
      throw new VeloxException(e);
    }
  }

  public static JsonNode parseTree(String json) {
    try {
      return JSON.reader().readTree(json);
    } catch (IOException e) {
      throw new VeloxException(e);
    }
  }

  public static <T extends NativeBean> T fromJson(String json, Class<? extends T> valueType) {
    try {
      return JSON.reader().readValue(json, valueType);
    } catch (IOException e) {
      throw new VeloxException(e);
    }
  }
}

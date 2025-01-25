package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.zhztheplayer.velox4j.exception.VeloxException;

import java.io.IOException;

public final class JsonSerde {
  private static final ObjectMapper JSON = new ObjectMapper();

  public static String toJson(Object object) {
    try {
      return JSON.writer().writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new VeloxException(e);
    }
  }

  public static String toPrettyJson(Object object) {
    try {
      return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new VeloxException(e);
    }
  }

  public static <T> T fromJson(String json, Class<T> clazz) {
    try {
      return JSON.reader().readValue(json, clazz);
    } catch (IOException e) {
      throw new VeloxException(e);
    }
  }
}

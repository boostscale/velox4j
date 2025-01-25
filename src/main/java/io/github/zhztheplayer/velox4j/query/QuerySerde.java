package io.github.zhztheplayer.velox4j.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class QuerySerde {
  private static final ObjectMapper JSON = new ObjectMapper();

  public static String toJson(Query query) {
    try {
      return JSON.writer().writeValueAsString(query);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String toPrettyJson(Query query) {
    try {
      return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(query);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}

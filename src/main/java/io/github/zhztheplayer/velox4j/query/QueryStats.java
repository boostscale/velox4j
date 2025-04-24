package io.github.zhztheplayer.velox4j.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.serde.Serde;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class QueryStats {
  private final ArrayNode planStatsDynamic;

  private QueryStats(JsonNode planStatsDynamic) {
    this.planStatsDynamic = (ArrayNode) planStatsDynamic;
  }

  public static QueryStats fromJson(String statsJson) {
    final JsonNode dynamic = Serde.parseTree(statsJson);
    final JsonNode planStatsDynamic = Preconditions.checkNotNull(dynamic.get("planStats"),
        "Plan statistics not found in query statistics");
    return new QueryStats(planStatsDynamic);
  }

  public ObjectNode planStats(String planNodeId) {
    final List<ObjectNode> out = new ArrayList<>();
    for (JsonNode each : planStatsDynamic) {
      if (Objects.equals(each.get("planNodeId").asText(), planNodeId)) {
        out.add((ObjectNode) each);
      }
    }
    if (out.isEmpty()) {
      throw new VeloxException(String.format("Query plan statistics for plan node not found, node ID: %s", planNodeId));
    }
    if (out.size() != 1) {
      throw new VeloxException(String.format("More than one nodes (%d total) with the same node ID found in query plan statistics, node ID: %s", out.size(), planNodeId));
    }
    return out.get(0);
  }
}

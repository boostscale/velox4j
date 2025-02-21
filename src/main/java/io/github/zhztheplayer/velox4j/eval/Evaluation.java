package io.github.zhztheplayer.velox4j.eval;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.serializable.VeloxSerializable;


public class Evaluation extends VeloxSerializable {
  private final TypedExpr expr;
  private final Config queryConfig;
  private final ConnectorConfig connectorConfig;

  @JsonCreator
  public Evaluation(
      @JsonProperty("expr") TypedExpr expr,
      @JsonProperty("queryConfig") Config queryConfig,
      @JsonProperty("connectorConfig") ConnectorConfig connectorConfig) {
    this.expr = expr;
    this.queryConfig = queryConfig;
    this.connectorConfig = connectorConfig;
  }

  @JsonGetter("expr")
  public TypedExpr expr() {
    return expr;
  }

  @JsonGetter("queryConfig")
  public Config queryConfig() {
    return queryConfig;
  }

  @JsonGetter("connectorConfig")
  public ConnectorConfig connectorConfig() {
    return connectorConfig;
  }
}

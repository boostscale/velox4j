package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ExternalStreamTableHandle extends ConnectorTableHandle {
  @JsonCreator
  public ExternalStreamTableHandle(@JsonProperty("connectorId") String connectorId) {
    super(connectorId);
  }
}

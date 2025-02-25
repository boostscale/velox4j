package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonGetter;
import io.github.zhztheplayer.velox4j.serializable.ISerializable;

public abstract class ConnectorTableHandle extends ISerializable {
  private final String connectorId;

  protected ConnectorTableHandle(String connectorId) {
    this.connectorId = connectorId;
  }

  @JsonGetter("connectorId")
  public String getConnectorId() {
    return connectorId;
  }
}

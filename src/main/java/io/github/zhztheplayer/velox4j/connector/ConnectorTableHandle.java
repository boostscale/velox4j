package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonGetter;
import io.github.zhztheplayer.velox4j.bean.VeloxBean;

public abstract class ConnectorTableHandle extends VeloxBean {
  private final String connectorId;

  public ConnectorTableHandle(String connectorId) {
    this.connectorId = connectorId;
  }

  @JsonGetter("connectorId")
  public String getConnectorId() {
    return connectorId;
  }
}

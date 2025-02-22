package io.github.zhztheplayer.velox4j.connector;

import io.github.zhztheplayer.velox4j.serializable.VeloxSerializable;

public abstract class ConnectorInsertTableHandle extends VeloxSerializable {
  protected ConnectorInsertTableHandle() {
  }

  public abstract boolean supportsMultiThreading();
}

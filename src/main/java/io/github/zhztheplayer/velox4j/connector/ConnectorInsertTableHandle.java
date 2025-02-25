package io.github.zhztheplayer.velox4j.connector;

import io.github.zhztheplayer.velox4j.serializable.ISerializable;

public abstract class ConnectorInsertTableHandle extends ISerializable {
  protected ConnectorInsertTableHandle() {
  }

  public abstract boolean supportsMultiThreading();
}

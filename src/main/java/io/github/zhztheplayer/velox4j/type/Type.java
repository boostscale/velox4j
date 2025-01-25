package io.github.zhztheplayer.velox4j.type;

import io.github.zhztheplayer.velox4j.serde.VeloxBean;

public abstract class Type extends VeloxBean {
  public Type(String key) {
    super(key);
  }
}

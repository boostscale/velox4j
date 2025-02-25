package io.github.zhztheplayer.velox4j.filter;

import com.fasterxml.jackson.annotation.JsonGetter;
import io.github.zhztheplayer.velox4j.serializable.ISerializable;

public abstract class Filter extends ISerializable {
  private final boolean nullAllowed;
  private final String kind;

  protected Filter(boolean nullAllowed, String kind) {
    this.nullAllowed = nullAllowed;
    this.kind = kind;
  }

  @JsonGetter("nullAllowed")
  public boolean isNullAllowed() {
    return nullAllowed;
  }

  @JsonGetter("kind")
  public String getKind() {
    return kind;
  }
}

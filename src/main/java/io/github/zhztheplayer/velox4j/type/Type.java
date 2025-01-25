package io.github.zhztheplayer.velox4j.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class Type {
  private final String name;
  private final String type;

  @JsonCreator
  protected Type(@JsonProperty("name") String name, @JsonProperty("type") String type) {
    this.name = name;
    this.type = type;
  }

  @JsonGetter("name")
  public String getName() {
    return name;
  }

  @JsonGetter("type")
  public String getType() {
    return type;
  }
}

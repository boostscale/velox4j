package io.github.zhztheplayer.velox4j.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class RowType extends Type {
  private final List<String> names;
  private final List<Type> children;

  @JsonCreator
  public RowType(@JsonProperty("names") List<String> names,
      @JsonProperty("cTypes") List<Type> children) {
    this.names = names;
    this.children = children;
  }

  @JsonProperty("names")
  public List<String> getNames() {
    return names;
  }

  @JsonProperty("cTypes")
  public List<Type> getChildren() {
    return children;
  }
}

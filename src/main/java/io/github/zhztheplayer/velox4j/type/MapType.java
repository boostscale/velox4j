package io.github.zhztheplayer.velox4j.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.List;

public class MapType extends Type {
  private final List<Type> children;

  @JsonCreator
  public MapType(@JsonProperty("cTypes") List<Type> children) {
    Preconditions.checkArgument(children.size() == 2,
        "MapType should have 2 children, but has %s", children.size());
    this.children = children;
  }

  @JsonGetter("cTypes")
  public List<Type> getChildren() {
    return children;
  }
}

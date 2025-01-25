package io.github.zhztheplayer.velox4j.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ArrayType extends SimpleType {
  private final List<Type> children;

  @JsonCreator
  public ArrayType(@JsonProperty("cTypes") List<Type> children) {
    super("ARRAY");
    this.children = children;
  }

  @JsonGetter("cTypes")
  public List<Type> getChildren() {
    return children;
  }
}

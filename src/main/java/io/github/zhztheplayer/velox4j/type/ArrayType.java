package io.github.zhztheplayer.velox4j.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;

public class ArrayType extends SimpleType {
  private final List<Type> children;

  @JsonCreator
  public ArrayType(@JsonProperty("cTypes") List<Type> children) {
    super("ARRAY");
    this.children = children;
  }

  @JsonGetter("cTypes")
  @JsonDeserialize(contentAs = Type.class)
  public List<Type> getChildren() {
    return children;
  }
}

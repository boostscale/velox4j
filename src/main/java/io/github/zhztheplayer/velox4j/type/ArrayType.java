package io.github.zhztheplayer.velox4j.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;

public class ArrayType extends Type {
  private final List<Type> children;

  @JsonCreator
  private ArrayType(@JsonProperty("cTypes") List<Type> children) {
    Preconditions.checkArgument(children.size() == 1,
        "ArrayType should have 1 child, but has %s", children.size());
    this.children = children;
  }

  public static ArrayType create(Type child) {
    return new ArrayType(Collections.singletonList(child));
  }

  @JsonGetter("cTypes")
  public List<Type> getChildren() {
    return children;
  }
}

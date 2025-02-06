package io.github.zhztheplayer.velox4j.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.exception.VeloxException;

import java.util.List;

public class RowType extends Type {
  private final List<String> names;
  private final List<Type> children;

  @JsonCreator
  public RowType(@JsonProperty("names") List<String> names,
      @JsonProperty("cTypes") List<Type> children) {
    Preconditions.checkArgument(names.size() == children.size(),
        "RowType should have same number of names and children");
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

  public int size() {
    return names.size();
  }

  public Type findChild(String name) {
    int index = names.indexOf(name);
    if (index == -1) {
      throw new VeloxException("Field " + name + " not found in RowType");
    }
    return children.get(index);
  }
}

package io.github.zhztheplayer.velox4j.filter;

import com.fasterxml.jackson.annotation.JsonCreator;

public class AlwaysTrue extends Filter {
  @JsonCreator
  public AlwaysTrue() {
    super(true, "kAlwaysTrue");
  }
}

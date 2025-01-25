package io.github.zhztheplayer.velox4j.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DecimalType extends Type {
  private final int precision;
  private final int scale;

  @JsonCreator
  public DecimalType(@JsonProperty("precision") int precision,
      @JsonProperty("scale") int scale) {
    this.precision = precision;
    this.scale = scale;
  }

  @JsonGetter("precision")
  public int getPrecision() {
    return precision;
  }

  @JsonGetter("scale")
  public int getScale() {
    return scale;
  }
}

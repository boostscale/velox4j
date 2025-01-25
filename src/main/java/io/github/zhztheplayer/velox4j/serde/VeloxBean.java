package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = VeloxBeanSerializer.class)
@JsonDeserialize(using = VeloxBeanDeserializer.class)
public abstract class VeloxBean {
  private final String key;

  @JsonCreator
  public VeloxBean(@JsonProperty("name") String key) {
    this.key = key;
  }

  @JsonGetter("name")
  public String getKey() {
    return key;
  }
}

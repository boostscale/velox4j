package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DoubleValue extends Variant {
    private final double value;

    @JsonCreator
    public DoubleValue(@JsonProperty("value") double value) {
        this.value = value;
    }

    @JsonGetter("value")
    public double isValue() {
        return value;
    }
}
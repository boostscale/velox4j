package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SmallIntValue extends Variant {
    private final int value;

    @JsonCreator
    public SmallIntValue(@JsonProperty("value") int value) {
        this.value = value;
    }

    @JsonGetter("value")
    public int getValue() {
        return value;
    }
}
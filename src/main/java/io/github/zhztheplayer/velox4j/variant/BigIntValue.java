package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BigIntValue extends Variant {
    private final long value;

    @JsonCreator
    public BigIntValue(@JsonProperty("value") long value) {
        this.value = value;
    }

    @JsonGetter("value")
    public long getValue() {
        return value;
    }
}
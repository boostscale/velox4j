package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigInteger;

public class HugeIntValue extends Variant {
    private final BigInteger value;

    @JsonCreator
    public HugeIntValue(@JsonProperty("value") BigInteger value) {
        this.value = value;
    }

    @JsonGetter("value")
    public BigInteger getValue() {
        return value;
    }
}

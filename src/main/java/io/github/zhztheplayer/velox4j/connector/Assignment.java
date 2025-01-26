package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Assignment {
    private final String assign;
    private final ColumnHandle columnHandle;

    @JsonCreator
    public Assignment(
        @JsonProperty("assign") String assign,
        @JsonProperty("columnHandle") ColumnHandle columnHandle) {
      this.assign = assign;
      this.columnHandle = columnHandle;
    }
    
    @JsonGetter("assign")
    public String getAssign() {
        return assign;
    }

    @JsonGetter("columnHandle")
    public ColumnHandle getColumnHandle() {
        return columnHandle;
    }
  }
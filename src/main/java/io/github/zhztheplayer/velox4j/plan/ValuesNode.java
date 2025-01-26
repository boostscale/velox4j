package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.List;

public class ValuesNode extends PlanNode {
  private final String serializedVectors;
  private final boolean parallelizable;
  private final int repeatTimes;

  @JsonCreator
  public ValuesNode(@JsonProperty("id") String id,
      @JsonProperty("data") String serializedVectors,
      @JsonProperty("parallelizable") boolean parallelizable,
      @JsonProperty("repeatTimes") int repeatTimes) {
    super(id);
    this.serializedVectors = Preconditions.checkNotNull(serializedVectors);
    this.parallelizable = parallelizable;
    this.repeatTimes = repeatTimes;
  }

  @JsonGetter("data")
  public String getSerializedVectors() {
    return serializedVectors;
  }

  @JsonGetter("parallelizable")
  public boolean isParallelizable() {
    return parallelizable;
  }

  @JsonGetter("repeatTimes")
  public int getRepeatTimes() {
    return repeatTimes;
  }

  @Override
  protected List<PlanNode> getSources() {
    return List.of();
  }
}

package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;

public class HiveInsertFileNameGenerator extends FileNameGenerator {
  @JsonCreator
  public HiveInsertFileNameGenerator() {
  }
}

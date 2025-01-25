package io.github.zhztheplayer.velox4j.plan;

public abstract class PlanNode {
  private final String name;
  private final String id;

  protected PlanNode(String name, String id) {
    this.name = name;
    this.id = id;
  }
}

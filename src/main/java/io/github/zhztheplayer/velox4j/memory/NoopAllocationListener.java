package io.github.zhztheplayer.velox4j.memory;

public class NoopAllocationListener implements AllocationListener {
  NoopAllocationListener() {}

  @Override
  public void allocationChanged(long diff) {}
}

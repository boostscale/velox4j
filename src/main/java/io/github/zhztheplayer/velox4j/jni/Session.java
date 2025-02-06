package io.github.zhztheplayer.velox4j.jni;

public class Session {
  private final long id;

  public Session(long id) {
    this.id = id;
  }

  public long getId() {
    return id;
  }
}

package io.github.zhztheplayer.velox4j.jni;

public class LocalSession implements Session {
  private final long id;

  private LocalSession(long id) {
    this.id = id;
  }

  static Session create(long id) {
    return new LocalSession(id);
  }

  @Override
  public long id() {
    return id;
  }
}

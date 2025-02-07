package io.github.zhztheplayer.velox4j.jni;

public class StaticSession implements Session {
  private static final StaticSession INSTANCE = new StaticSession();

  private StaticSession() {}

  static Session get() {
    return INSTANCE;
  }
  @Override
  public long id() {
    throw new UnsupportedOperationException("Static session doesn't have an ID assigned");
  }
}

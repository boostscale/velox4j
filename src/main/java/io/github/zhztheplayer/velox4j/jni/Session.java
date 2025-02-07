package io.github.zhztheplayer.velox4j.jni;

public class Session implements CppObject {
  private final long id;

  private Session(long id) {
    this.id = id;
  }

  public static Session create() {
    return new Session(JniWrapper.getStaticInstance().createSession());
  }

  @Override
  public long id() {
    return id;
  }
}

package io.github.zhztheplayer.velox4j.jni;

public class Session {
  private final long id;

  private Session(long id) {
    this.id = id;
  }

  public static Session create() {
    return new Session(JniWrapper.getStaticInstance().createSession());
  }

  public long getId() {
    return id;
  }
}

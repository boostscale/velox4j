package io.github.zhztheplayer.velox4j.connector;

import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.jni.JniApi;

public class ExternalStreams {
  private final JniApi jniApi;

  public ExternalStreams(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public ExternalStream bind(DownIterator itr) {
    return jniApi.newExternalStream(itr);
  }
}

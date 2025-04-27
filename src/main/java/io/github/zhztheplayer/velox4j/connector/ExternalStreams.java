package io.github.zhztheplayer.velox4j.connector;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.jni.CppObject;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;

public class ExternalStreams {
  private final JniApi jniApi;

  public ExternalStreams(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public ExternalStream bind(DownIterator itr) {
    return jniApi.createExternalStreamFromDownIterator(itr);
  }

  public BlockingQueue newBlockingQueue() {
    return jniApi.createBlockingQueue();
  }

  public static class GenericExternalStream implements ExternalStream {
    private final long id;

    public GenericExternalStream(long id) {
      this.id = id;
    }

    @Override
    public long id() {
      return id;
    }
  }

  public static class BlockingQueue implements ExternalStream {
    private final long id;

    public BlockingQueue(long id) {
      this.id = id;
    }

    @Override
    public long id() {
      return id;
    }

    public void put(RowVector rowVector) {
      StaticJniApi.get().blockingQueuePut(this, rowVector);
    }

    public void noMoreInput() {
      StaticJniApi.get().blockingQueueNoMoreInput(this);
    }
  }
}

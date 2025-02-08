/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.zhztheplayer.velox4j.jni;

import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;

final class JniWrapper {
  private static final JniWrapper STATIC_INSTANCE = new JniWrapper(StaticSession.get());

  static JniWrapper staticInstance() {
    return STATIC_INSTANCE;
  }

  static JniWrapper create(long memoryManagerId) {
    final Session session = LocalSession.create(JniWrapper.staticInstance().createSession(memoryManagerId));
    return new JniWrapper(session);
  }

  private final Session session;

  private JniWrapper(Session session) {
    this.session = session;
  }

  void close() {
    session.close();
  }

  @CalledFromNative
  public long sessionId() {
    return session.id();
  }

  // Memory.
  native long createMemoryManager(AllocationListener listener);

  // Lifecycle.
  native long createSession(long memoryManagerId);

  native void releaseCppObject(long objectId);

  // Plan execution.
  native long executeQuery(String jsonQuery);

  // For UpIterator.
  native boolean upIteratorHasNext(long address);

  native long upIteratorNext(long address);

  // For DownIterator.
  native long downIteratorAsExternalStream(DownIterator itr);

  // For Variant.
  native String variantInferType(String json);

  // For BaseVector / RowVector.
  native long arrowToBaseVector(long cSchema, long cArray);

  native void baseVectorToArrow(long rvAddress, long cSchema, long cArray);

  native String baseVectorSerialize(long[] id);

  native long[] baseVectorDeserialize(String serialized);

  native String baseVectorGetType(long id);

  native long baseVectorWrapInConstant(long id, int length, int index);

  native String baseVectorGetEncoding(long id);

  native long baseVectorNewRef(long id);

  // For tests.
  native String deserializeAndSerialize(String json);

  native String deserializeAndSerializeVariant(String json);

  native long createUpIteratorWithExternalStream(long id);
}

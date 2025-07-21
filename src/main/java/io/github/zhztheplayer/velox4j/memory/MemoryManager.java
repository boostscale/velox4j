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
package io.github.zhztheplayer.velox4j.memory;

import io.github.zhztheplayer.velox4j.jni.CppObject;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;

/**
 * A memory manager for Velox4J. One memory manager can be bound to different Velox4J sessions or to
 * only one session per user's need.
 *
 * <p>MemoryManager should be closed after use, as it's a CppObject. Once being closed, a memory
 * leakage check will be performed, that says, all the sessions that are managed by this memory
 * manager should be gracefully released (by calling Session#close) then this memory manager can be
 * closed without any leakage. Error will be thrown otherwise.
 */
public class MemoryManager implements CppObject {

  /**
   * Creates a memory manager instance with a given {@link AllocationListener}. The listener will
   * listen on all the native memory allocations.
   */
  public static MemoryManager create(AllocationListener listener) {
    return StaticJniApi.get().createMemoryManager(listener);
  }

  private final long id;

  public MemoryManager(long id) {
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }
}

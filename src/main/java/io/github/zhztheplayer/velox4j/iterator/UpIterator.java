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
package io.github.zhztheplayer.velox4j.iterator;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.jni.CppObject;

/**
 * An up-iterator is the opposite of down-iterator. It transmits data that is output from Velox
 * pipeline from C++ to Java.
 */
public interface UpIterator extends CppObject {
  enum State {
    AVAILABLE(0),
    BLOCKED(1),
    FINISHED(2);

    private static final Map<Integer, State> STATE_ID_LOOKUP = new HashMap<>();

    static {
      for (State state : State.values()) {
        STATE_ID_LOOKUP.put(state.id, state);
      }
    }

    public static State get(int id) {
      Preconditions.checkArgument(
          STATE_ID_LOOKUP.containsKey(id), String.format("ID not found: %d", id));
      return STATE_ID_LOOKUP.get(id);
    }

    private final int id;

    State(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }
  }

  /** Gets the next state. */
  State advance();

  /**
   * Called once `advance` returns `BLOCKED` state to wait until the state gets refreshed, either by
   * the next row-vector is ready for reading or by end of stream.
   */
  void waitFor();

  /** Called to close the iterator. */
  RowVector get();
}

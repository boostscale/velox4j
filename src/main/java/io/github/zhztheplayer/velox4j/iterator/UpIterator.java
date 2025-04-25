package io.github.zhztheplayer.velox4j.iterator;

import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.jni.CppObject;

import java.util.HashMap;
import java.util.Map;

public interface UpIterator extends CppObject {
  enum State {
    AVAILABLE(0),
    BLOCKED(1),
    FINISHED(2);

    private final static Map<Integer, State> STATE_ID_LOOKUP = new HashMap<>();

    static {
      for (State state : State.values()) {
        STATE_ID_LOOKUP.put(state.id, state);
      }
    }

    public static State get(int id) {
      Preconditions.checkArgument(STATE_ID_LOOKUP.containsKey(id), "ID not found: %d", id);
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

  State advance();

  void waitFor();

  RowVector get();
}

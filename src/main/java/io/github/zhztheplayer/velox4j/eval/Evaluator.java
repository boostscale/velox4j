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
package io.github.zhztheplayer.velox4j.eval;

import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.data.SelectivityVector;
import io.github.zhztheplayer.velox4j.jni.CppObject;
import io.github.zhztheplayer.velox4j.jni.JniApi;

public class Evaluator implements CppObject {
  private final JniApi jniApi;
  private final long id;

  public Evaluator(JniApi jniApi, long id) {
    this.jniApi = jniApi;
    this.id = id;
  }

  public JniApi jniApi() {
    return jniApi;
  }

  @Override
  public long id() {
    return id;
  }

  public BaseVector eval(SelectivityVector sv, RowVector input) {
    return jniApi.evaluatorEval(this, sv, input);
  }
}

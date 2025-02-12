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

import io.github.zhztheplayer.velox4j.exception.VeloxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JniWorkspace {
  private static final Logger LOG = LoggerFactory.getLogger(JniWorkspace.class);
  private static final Map<String, JniWorkspace> INSTANCES = new ConcurrentHashMap<>();
  private static final String DEFAULT_ROOT_DIR;

  static {
    try {
      DEFAULT_ROOT_DIR = Files.createTempDirectory("velox4j-").toFile().getAbsolutePath();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final File workDir;

  private JniWorkspace(String rootDir) {
    try {
      LOG.info("Creating JNI workspace in root directory {}", rootDir);
      final Path root = Paths.get(rootDir);
      final Path work = Files.createTempDirectory(root, "work-").toAbsolutePath();
      this.workDir = work.toFile();
      LOG.info("JNI workspace {} created in root directory {}", this.workDir, rootDir);
    } catch (Exception e) {
      throw new VeloxException(e);
    }
  }

  public static JniWorkspace getDefault() {
    return createOrGet(DEFAULT_ROOT_DIR);
  }

  private static JniWorkspace createOrGet(String rootDir) {
    return INSTANCES.computeIfAbsent(rootDir, JniWorkspace::new);
  }

  public File getWorkDir() {
    return workDir;
  }
}

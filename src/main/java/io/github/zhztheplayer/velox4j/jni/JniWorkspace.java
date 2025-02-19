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

import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JniWorkspace {
  private static final Logger LOG = LoggerFactory.getLogger(JniWorkspace.class);
  private static final Map<String, JniWorkspace> INSTANCES = new ConcurrentHashMap<>();

  private static final String DEFAULT_WORK_DIR_MAGIC = "1609902915266301671";
  private static final String DEFAULT_WORK_DIR;

  static {
    final File defaultWorkDir = new File(FileUtils.getTempDirectoryPath(),
        String.format("velox4j-%s", DEFAULT_WORK_DIR_MAGIC));
    final String defaultWorkDirPath = defaultWorkDir.getAbsolutePath();
    if (defaultWorkDir.exists()) {
      Preconditions.checkState(defaultWorkDir.isDirectory(),
          "Default work directory %s already exists but is not recognized as a directory", defaultWorkDirPath);
      System.out.printf("Deleting existing contents in work directory %s...%n", defaultWorkDirPath);
      try {
        FileUtils.deleteDirectory(defaultWorkDir);
      } catch (IOException e) {
        throw new VeloxException(e);
      }
    }
    System.out.printf("Creating work directory %s...%n", defaultWorkDirPath);
    Preconditions.checkState(defaultWorkDir.mkdirs(), "Cannot create work directory %s", defaultWorkDirPath);
    DEFAULT_WORK_DIR = defaultWorkDirPath;
  }

  private final File workDir;

  private JniWorkspace(File workDir) {
    try {
      this.workDir = workDir;
      mkdirs(this.workDir);
      LOG.info("JNI workspace created in directory {}", this.workDir);
    } catch (Exception e) {
      throw new VeloxException(e);
    }
  }

  public static JniWorkspace getDefault() {
    return createOrGet(DEFAULT_WORK_DIR);
  }

  private static JniWorkspace createOrGet(String workDir) {
    return INSTANCES.computeIfAbsent(workDir, d -> new JniWorkspace(new File(d)));
  }

  public File getSubDir(String subDir) {
    final File file = new File(this.workDir, subDir);
    mkdirs(file);
    return file;
  }

  private static void mkdirs(File dir) {
    if (!dir.exists()) {
      Preconditions.checkState(dir.mkdirs(), "Failed to create directory %s", dir);
    }
    Preconditions.checkArgument(dir.isDirectory(), "File %s is not a directory", dir);
  }
}

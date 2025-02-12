package io.github.zhztheplayer.velox4j.resource;

import java.io.File;

/**
 * Represents a virtual file view of a resource file found in classpath.
 */
public interface ResourceFile {
  String container();
  String name();
  void copyTo(File file);
}

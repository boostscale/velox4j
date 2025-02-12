package io.github.zhztheplayer.velox4j.resource;

import java.io.File;

public class PlainResourceFile implements ResourceFile {
  private final String container;
  private final String name;

  PlainResourceFile(String container, String name) {
    this.container = container;
    this.name = name;
  }

  @Override
  public String container() {
    return container;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void copyTo(File file) {
    Resources.copyResource(container + File.separator + name, file);
  }
}

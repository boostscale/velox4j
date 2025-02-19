package io.github.zhztheplayer.velox4j.jni;

import com.google.common.base.Preconditions;
import com.google.common.base.StandardSystemProperty;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.resource.ResourceFile;
import io.github.zhztheplayer.velox4j.resource.Resources;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JniLibLoader {
  private static final AtomicBoolean LOADED = new AtomicBoolean(false);

  private static final String LIB_CONTAINER = String.format("velox4j-lib/%s/%s", StandardSystemProperty.OS_NAME.value(), StandardSystemProperty.OS_ARCH.value());
  private static final Pattern LIB_PATTERN = Pattern.compile("^.+$");
  private static final String VELOX4J_LIB_NAME = "libvelox4j.so";

  public static void loadAll(File workDir) {
    if (!LOADED.compareAndSet(false, true)) {
      throw new VeloxException("Libraries were already loaded");
    }
    Preconditions.checkArgument(workDir.isDirectory(), "Work directory %s is not a directory", workDir);
    final List<ResourceFile> libFiles = Resources.getResources(LIB_CONTAINER, LIB_PATTERN);
    if (libFiles.isEmpty()) {
      throw new VeloxException(String.format("Library container %s not found in classpath. Please check whether the current platform is supported by the Jar", LIB_CONTAINER));
    }
    final List<ResourceFile> velox4jLibFiles = libFiles.stream().filter(f -> f.name().equals(VELOX4J_LIB_NAME)).collect(Collectors.toList());
    Preconditions.checkArgument(velox4jLibFiles.size() == 1, "Velox4j library not found");
    System.out.printf("Found required libraries in container %s.%n", LIB_CONTAINER);
    final ResourceFile velox4jLibFile = velox4jLibFiles.get(0);
    for (ResourceFile libFile : libFiles) {
      final File copied = workDir.toPath().resolve(libFile.name()).toFile();
      System.out.printf("Copying library %s/%s to %s...%n", LIB_CONTAINER, libFile.name(), copied);
      libFile.copyTo(copied);
    }
    final File copiedVelox4jLib = workDir.toPath().resolve(velox4jLibFile.name()).toFile();
    Preconditions.checkState(copiedVelox4jLib.isFile(), "Velox4j library not copied to work directory");
    System.load(copiedVelox4jLib.getAbsolutePath());
    System.out.printf("All required libraries were successfully loaded.%n");
  }
}

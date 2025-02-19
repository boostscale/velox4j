package io.github.zhztheplayer.velox4j.test;

import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.jni.JniWorkspace;
import io.github.zhztheplayer.velox4j.resource.Resources;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public final class ResourceTests {
  public static String readResourceAsString(String path) {
    final ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    try (final InputStream is = classloader.getResourceAsStream(path)) {
      Preconditions.checkArgument(is != null, "Resource %s not found", path);
      final ByteArrayOutputStream o = new ByteArrayOutputStream();
      while (true) {
        int b = is.read();
        if (b == -1) {
          break;
        }
        o.write(b);
      }
      final byte[] bytes = o.toByteArray();
      o.close();
      return new String(bytes, Charset.defaultCharset());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static File copyResourceToTmp(String path) {
    try {
      final File folder = JniWorkspace.getDefault().getSubDir("test");
      final File tmp = File.createTempFile("velox4j-test-", ".tmp", folder);
      Resources.copyResource(path, tmp);
      return tmp;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

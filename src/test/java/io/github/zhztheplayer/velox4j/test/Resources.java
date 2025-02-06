package io.github.zhztheplayer.velox4j.test;

import com.google.common.base.Preconditions;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public final class Resources {
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
    final ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    try (final InputStream is = classloader.getResourceAsStream(path)) {
      Preconditions.checkArgument(is != null, "Resource %s not found", path);
      final File tmp = File.createTempFile("velox4j-test-", ".tmp");
      final BufferedOutputStream o = new BufferedOutputStream(new FileOutputStream(tmp));
      while (true) {
        int b = is.read();
        if (b == -1) {
          break;
        }
        o.write(b);
      }
      o.flush();
      o.close();
      return tmp;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

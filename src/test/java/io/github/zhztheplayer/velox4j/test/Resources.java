package io.github.zhztheplayer.velox4j.test;

import com.google.common.base.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public final class Resources {
  public static String readTextResource(String path) {
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
}

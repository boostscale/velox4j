package io.github.zhztheplayer.velox4j.bean;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanSerializer;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.std.BeanSerializerBase;
import io.github.zhztheplayer.velox4j.serde.SerdeRegistry;

import java.io.IOException;
import java.util.List;

public class VeloxBeanSerializer extends BeanSerializer {
  private VeloxBeanSerializer(BeanSerializerBase base) {
    super(base);
  }

  @Override
  public void serialize(Object bean, JsonGenerator gen, SerializerProvider provider) throws IOException {
    final Class<?> clazz = bean.getClass();
    if (SerdeRegistry.isRegistered(clazz)) {
      final List<SerdeRegistry.KvPair> kvs = SerdeRegistry.findKvPairs(clazz);
      for (SerdeRegistry.KvPair kv : kvs) {
        gen.writeStringField(kv.getKey(), kv.getValue());
      }
    }
    super.serialize(bean, gen, provider);
  }

  public static class Modifier extends BeanSerializerModifier {
    @Override
    public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription beanDesc, JsonSerializer<?> serializer) {
      return new VeloxBeanSerializer((BeanSerializerBase) serializer);
    }
  }
}

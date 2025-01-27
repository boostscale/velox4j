package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanSerializer;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.std.BeanSerializerBase;
import com.fasterxml.jackson.databind.ser.std.ToEmptyObjectSerializer;
import io.github.zhztheplayer.velox4j.bean.VeloxBean;

import java.io.IOException;
import java.util.List;

public final class PolymorphicSerializer {
  private PolymorphicSerializer() {
  }

  private static class EmptyBeanSerializer extends JsonSerializer<Object> {
    @Override
    public void serialize(Object bean, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      gen.writeStartObject();
      final Class<?> clazz = bean.getClass();
      final List<SerdeRegistry.KvPair> kvs = SerdeRegistry.findKvPairs(clazz);
      for (SerdeRegistry.KvPair kv : kvs) {
        gen.writeStringField(kv.getKey(), kv.getValue());
      }
      gen.writeEndObject();
    }
  }

  private static final class NonEmptyBeanSerializer extends BeanSerializer {
    public NonEmptyBeanSerializer(BeanSerializerBase base) {
      super(base);
    }

    @Override
    protected void serializeFields(Object bean, JsonGenerator gen, SerializerProvider provider) throws IOException {
      final Class<?> clazz = bean.getClass();
      final List<SerdeRegistry.KvPair> kvs = SerdeRegistry.findKvPairs(clazz);
      for (SerdeRegistry.KvPair kv : kvs) {
        gen.writeStringField(kv.getKey(), kv.getValue());
      }
      super.serializeFields(bean, gen, provider);
    }
  }

  public static class Modifier extends BeanSerializerModifier {
    private final Class<?> baseClass;

    public Modifier(Class<?> baseClass) {
      this.baseClass = baseClass;
    }

    @Override
    public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription beanDesc, JsonSerializer<?> serializer) {
      if (baseClass.isAssignableFrom(beanDesc.getBeanClass())) {
        if (serializer instanceof ToEmptyObjectSerializer) {
          return new EmptyBeanSerializer();
        }
        if (serializer instanceof BeanSerializerBase) {
          return new NonEmptyBeanSerializer(((BeanSerializerBase) serializer));
        }
      }
      return serializer;
    }
  }
}

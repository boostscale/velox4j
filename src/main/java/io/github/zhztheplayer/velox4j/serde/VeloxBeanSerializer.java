package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanSerializer;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.std.BeanSerializerBase;

import java.io.IOException;

public class VeloxBeanSerializer extends BeanSerializer {
  protected VeloxBeanSerializer(BeanSerializerBase src) {
    super(src);
  }

  @Override
  protected void serializeFields(Object bean, JsonGenerator gen, SerializerProvider provider) throws IOException {
    if (bean instanceof VeloxBean) {
      final String key = VeloxBeanRegistry.findKeyByClass((Class<? extends VeloxBean>) bean.getClass());
      gen.writeStringField("name", key);
    }
    super.serializeFields(bean, gen, provider);
  }

  @Override
  protected void serializeFieldsFiltered(Object bean, JsonGenerator gen, SerializerProvider provider) throws IOException {
    if (bean instanceof VeloxBean) {
      final String key = VeloxBeanRegistry.findKeyByClass((Class<? extends VeloxBean>) bean.getClass());
      gen.writeStringField("name", key);
    }
    super.serializeFieldsFiltered(bean, gen, provider);
  }

  public static class Modifier extends BeanSerializerModifier {
    @Override
    public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription beanDesc, JsonSerializer<?> serializer) {
      if (VeloxBean.class.isAssignableFrom(beanDesc.getBeanClass())) {
        return new VeloxBeanSerializer((BeanSerializer) serializer);
      }
      return super.modifySerializer(config, beanDesc, serializer);
    }
  }
}

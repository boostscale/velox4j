package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializer;
import com.fasterxml.jackson.databind.ser.BeanSerializerBuilder;

import java.io.IOException;

public class VeloxBeanSerializer extends BeanSerializer {
  public VeloxBeanSerializer(JavaType type, BeanSerializerBuilder builder, BeanPropertyWriter[] properties, BeanPropertyWriter[] filteredProperties) {
    super(type, builder, properties, filteredProperties);
  }

  @Override
  protected void serializeFields(Object bean, JsonGenerator gen, SerializerProvider provider) throws IOException {
    super.serializeFields(bean, gen, provider);
    gen.writeStringField("FOO", "BAR");
  }
}

package io.github.zhztheplayer.velox4j.bean;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class VeloxBeanDeserializer extends JsonDeserializer<Object> {
  private final JsonDeserializer<?> base;

  public VeloxBeanDeserializer(JsonDeserializer<?> base) {
    this.base = base;
  }

  @Override
  public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
    final ObjectNode node = p.readValueAsTree();
    if (!node.has("name")) {
      return base.deserialize(p, ctxt);
    }
    final String key = node.get("name").asText();
    final Class<? extends VeloxBean> clazz = VeloxBeanRegistry.get().findClassByKey(key);
    return p.getCodec().treeToValue(node.without("name"), clazz);
  }

  public static class Modifier extends BeanDeserializerModifier {
    @Override
    public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config, BeanDescription beanDesc, JsonDeserializer<?> deserializer) {
      return new VeloxBeanDeserializer(deserializer);
    }
  }
}

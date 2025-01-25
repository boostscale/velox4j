package io.github.zhztheplayer.velox4j.bean;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.serde.SerdeRegistry;
import io.github.zhztheplayer.velox4j.serde.SerdeRegistryFactory;
import io.github.zhztheplayer.velox4j.stream.Streams;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class VeloxBeanDeserializer extends JsonDeserializer<Object> {

  public VeloxBeanDeserializer() {
  }

  private SerdeRegistry findRegistry(SerdeRegistryFactory rf, ObjectNode obj) {
    final Set<String> keys = rf.keys();
    final List<String> keysInObj = Streams.fromIterator(obj.fieldNames()).filter(keys::contains).collect(Collectors.toList());
    if (keysInObj.isEmpty()) {
      throw new UnsupportedOperationException("Required keys not found in JSON: " + obj);
    }

    if (keysInObj.size() > 1) {
      throw new UnsupportedOperationException("Ambiguous key annotations in JSON: " + obj);
    }
    final SerdeRegistry registry = rf.key(keysInObj.get(0));
    return registry;
  }

  private Object deserializeWithRegistry(JsonParser p, DeserializationContext ctxt, SerdeRegistry registry, ObjectNode objectNode) {
    final String key = registry.key();
    final String value = objectNode.remove(key).asText();
    Preconditions.checkArgument(registry.contains(value), "Value %s not registered in registry: %s", value, registry.prefixAndKey());
    if (registry.isFactory(value)) {
      final SerdeRegistryFactory rf = registry.getFactory(value);
      final SerdeRegistry nextRegistry = findRegistry(rf, objectNode);
      return deserializeWithRegistry(p, ctxt, nextRegistry, objectNode);
    }
    if (registry.isClass(value)) {
      Class<?> clazz = registry.getClass(value);
      try {
        return p.getCodec().treeToValue(objectNode, clazz);
      } catch (JsonProcessingException e) {
        throw new VeloxException(e);
      }
    }
    throw new IllegalStateException();
  }

  @Override
  public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
    final TreeNode treeNode = p.readValueAsTree();
    if (!treeNode.isObject()) {
      throw new UnsupportedOperationException("Not a JSON object: " + treeNode);
    }
    final ObjectNode objNode = (ObjectNode) treeNode;
    final SerdeRegistry registry = findRegistry(SerdeRegistryFactory.get(), objNode);
    return deserializeWithRegistry(p, ctxt, registry, objNode);
  }

  public static class Modifier extends BeanDeserializerModifier {
    @Override
    public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config, BeanDescription beanDesc, JsonDeserializer<?> deserializer) {
      if (VeloxBean.class.isAssignableFrom(beanDesc.getBeanClass())) {
        if (java.lang.reflect.Modifier.isAbstract(beanDesc.getBeanClass().getModifiers())) {
          // We only use the
          return new VeloxBeanDeserializer();
        }
      }
      return deserializer;
    }
  }
}

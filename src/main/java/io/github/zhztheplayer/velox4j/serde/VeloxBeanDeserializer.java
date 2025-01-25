package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.TextNode;
import io.github.zhztheplayer.velox4j.exception.VeloxException;

import java.io.IOException;

public class VeloxBeanDeserializer extends StdDeserializer<VeloxBean> {
  protected VeloxBeanDeserializer() {
    super(VeloxBean.class);
  }

  @Override
  public VeloxBean deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
    final TreeNode node = p.readValueAsTree();
    if (node.get("name") == null) {
      throw new VeloxException(String.format("Intended to find the bean type of JSON %s, but the key name is not presented",
          node));
    }
    final String key = ((TextNode) node.get("name")).asText();
    final Class<? extends VeloxBean> clazz = VeloxBeanRegistry.find(key);
    return p.getCodec().treeToValue(node, clazz);
  }
}

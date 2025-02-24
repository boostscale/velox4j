package com.fasterxml.jackson.databind.deser;

import java.util.Set;

public final class BeanDeserializerUnsafe {
  public static Set<String> getIgnorableProps(BeanDeserializer beanDeserializer) {
    return beanDeserializer._ignorableProps;
  }

  public static Set<String> getIncludableProps(BeanDeserializer beanDeserializer) {
    return beanDeserializer._includableProps;
  }
}

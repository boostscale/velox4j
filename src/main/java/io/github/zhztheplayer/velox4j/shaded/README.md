# The Java source package `java.io.github.zhztheplayer.velox4j.shaded`

This special package is for storing 3rd source code which cannot be correctly handled by the default Maven shader.

Arrow Java is a typical 3rd dependency that is difficult to shade since our work on Velox4J relies on the JNI packages
of Arrow Java. The C++ code in JNI libraries is pre-built before shipping to the Maven central so Maven shader cannot 
modify it.

As we don't have an elegant solution yet, we simply copy such code to this package and manually shade the package.
For clearance for developers, the origins of the shaded code are listed below:

## Shaded C Interfaces for Arrow Java

The shaded arrow code is from Arrow release 17.0.0 (the last version that supports Java 8):
https://github.com/apache/arrow/tree/apache-arrow-17.0.0/java/c.

### Modifications to the OSS Arrow C interface code that are worth to mention:

- Remove JniLoader.java.

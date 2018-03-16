[![Build Status][ci-img]][ci] [![Released Version][maven-img]][maven]

# OpenTracing Memcached Client Instrumentation
OpenTracing instrumentation for Memcached Client.

## Installation

### Spymemcached
pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-spymemcached</artifactId>
    <version>VERSION</version>
</dependency>
```

## Usage


```java
// Instantiate tracer
Tracer tracer = ...
```

### Spymemcached

```
// Create Tracing Memcached Client
MemcachedClient client = new TracingMemcachedClient(tracer, false,
        new InetSocketAddress("localhost", 11211))

// Set an object in the cache
client.set("key", 0, "value")
```


[ci-img]: https://travis-ci.org/opentracing-contrib/java-memcached-client.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-memcached-client
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-memcached-parent.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-memcached-parent

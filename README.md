[![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![Released Version][maven-img]][maven]

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
[cov-img]: https://coveralls.io/repos/github/opentracing-contrib/java-memcached-client/badge.svg?branch=master
[cov]: https://coveralls.io/github/opentracing-contrib/java-memcached-client?branch=master
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-memcached-parent.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-memcached-parent

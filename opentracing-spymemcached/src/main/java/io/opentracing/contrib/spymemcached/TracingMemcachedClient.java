/*
 * Copyright 2018 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing.contrib.spymemcached;

import static io.opentracing.contrib.spymemcached.TracingHelper.nullable;
import static io.opentracing.contrib.spymemcached.TracingHelper.nullableClass;
import static io.opentracing.contrib.spymemcached.TracingHelper.onError;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import net.spy.memcached.BroadcastOpFactory;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.TranscodeService;
import net.spy.memcached.transcoders.Transcoder;

public class TracingMemcachedClient extends MemcachedClient {

  private final TracingHelper helper;

  public TracingMemcachedClient(Tracer tracer, boolean traceWithActiveSpanOnly,
      InetSocketAddress... ia) throws IOException {
    super(ia);
    helper = new TracingHelper(tracer, traceWithActiveSpanOnly);
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingMemcachedClient(boolean traceWithActiveSpanOnly,
      InetSocketAddress... ia) throws IOException {
    this(GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingMemcachedClient(List<InetSocketAddress> addrs, Tracer tracer,
      boolean traceWithActiveSpanOnly) throws IOException {
    super(addrs);
    helper = new TracingHelper(tracer, traceWithActiveSpanOnly);
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingMemcachedClient(List<InetSocketAddress> addrs,
      boolean traceWithActiveSpanOnly) throws IOException {
    this(addrs, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  public TracingMemcachedClient(ConnectionFactory cf, List<InetSocketAddress> addrs, Tracer tracer,
      boolean traceWithActiveSpanOnly)
      throws IOException {
    super(cf, addrs);
    helper = new TracingHelper(tracer, traceWithActiveSpanOnly);
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingMemcachedClient(ConnectionFactory cf, List<InetSocketAddress> addrs,
      boolean traceWithActiveSpanOnly)
      throws IOException {
    this(cf, addrs, GlobalTracer.get(), traceWithActiveSpanOnly);
  }

  @Override
  public Collection<SocketAddress> getAvailableServers() {
    return super.getAvailableServers();
  }

  @Override
  public Collection<SocketAddress> getUnavailableServers() {
    return super.getUnavailableServers();
  }

  @Override
  public NodeLocator getNodeLocator() {
    return super.getNodeLocator();
  }

  @Override
  public Transcoder<Object> getTranscoder() {
    return super.getTranscoder();
  }

  @Override
  public CountDownLatch broadcastOp(BroadcastOpFactory of) {
    return super.broadcastOp(of);
  }

  @Override
  public CountDownLatch broadcastOp(BroadcastOpFactory of, Collection<MemcachedNode> nodes) {
    return super.broadcastOp(of, nodes);
  }

  @Override
  public <T> OperationFuture<Boolean> touch(String key, int expiration) {
    Span span = helper.buildSpan("touch", key);
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.touch(key, expiration).addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> OperationFuture<Boolean> touch(String key, int expiration, Transcoder<T> transcoder) {
    Span span = helper.buildSpan("touch", key);
    span.setTag("expiration", expiration);
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.touch(key, expiration, transcoder)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Boolean> append(long cas, String key, Object value) {
    Span span = helper.buildSpan("append", key);
    span.setTag("cas", cas);
    span.setTag("value", nullable(value));
    try (Scope ignore = helper.activate(span)) {
      return super.append(cas, key, value)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Boolean> append(String key, Object value) {
    Span span = helper.buildSpan("append", key);
    span.setTag("value", nullable(value));
    try (Scope ignore = helper.activate(span)) {
      return super.append(key, value).addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> OperationFuture<Boolean> append(long cas, String key, T value,
      Transcoder<T> transcoder) {
    Span span = helper.buildSpan("append", key);
    span.setTag("cas", cas);
    span.setTag("value", nullable(value));
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.append(cas, key, value, transcoder)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> OperationFuture<Boolean> append(String key, T value, Transcoder<T> transcoder) {
    Span span = helper.buildSpan("append", key);
    span.setTag("value", nullable(value));
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.append(key, value, transcoder)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Boolean> prepend(long cas, String key, Object value) {
    Span span = helper.buildSpan("prepend", key);
    span.setTag("cas", cas);
    span.setTag("value", nullable(value));
    try (Scope ignore = helper.activate(span)) {
      return super.prepend(cas, key, value)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Boolean> prepend(String key, Object value) {
    Span span = helper.buildSpan("prepend", key);
    span.setTag("value", nullable(value));
    try (Scope ignore = helper.activate(span)) {
      return super.prepend(key, value).addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> OperationFuture<Boolean> prepend(long cas, String key, T value,
      Transcoder<T> transcoder) {
    Span span = helper.buildSpan("prepend", key);
    span.setTag("cas", cas);
    span.setTag("value", nullable(value));
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.prepend(cas, key, value, transcoder)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> OperationFuture<Boolean> prepend(String key, T value, Transcoder<T> transcoder) {
    Span span = helper.buildSpan("prepend", key);
    span.setTag("value", nullable(value));
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.prepend(key, value, transcoder)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> OperationFuture<CASResponse> asyncCAS(String key, long casId, T value,
      Transcoder<T> transcoder) {
    Span span = helper.buildSpan("asyncCAS", key);
    span.setTag("casId", casId);
    span.setTag("value", nullable(value));
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.asyncCAS(key, casId, value, transcoder)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> OperationFuture<CASResponse> asyncCAS(String key, long casId, int expiration, T
      value,
      Transcoder<T> transcoder) {
    Span span = helper.buildSpan("asyncCAS", key);
    span.setTag("casId", casId);
    span.setTag("value", nullable(value));
    span.setTag("transcoder", nullableClass(transcoder));
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncCAS(key, casId, expiration, value, transcoder)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<CASResponse> asyncCAS(String key, long casId, Object value) {
    Span span = helper.buildSpan("asyncCAS", key);
    span.setTag("casId", casId);
    span.setTag("value", nullable(value));
    try (Scope ignore = helper.activate(span)) {
      return super.asyncCAS(key, casId, value)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<CASResponse> asyncCAS(String key, long casId, int expiration,
      Object value) {
    Span span = helper.buildSpan("asyncCAS", key);
    span.setTag("casId", casId);
    span.setTag("value", nullable(value));
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncCAS(key, casId, expiration, value)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> CASResponse cas(String key, long casId, T value, Transcoder<T> transcoder) {
    Span span = helper.buildSpan("cas", key);
    span.setTag("casId", casId);
    span.setTag("value", nullable(value));
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.cas(key, casId, value, transcoder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <T> CASResponse cas(String key, long casId, int expiration, T value,
      Transcoder<T> transcoder) {
    Span span = helper.buildSpan("cas", key);
    span.setTag("casId", casId);
    span.setTag("value", nullable(value));
    span.setTag("expiration", expiration);
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.cas(key, casId, expiration, value, transcoder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public CASResponse cas(String key, long casId, Object value) {
    Span span = helper.buildSpan("cas", key);
    span.setTag("casId", casId);
    span.setTag("value", nullable(value));
    try (Scope ignore = helper.activate(span)) {
      return super.cas(key, casId, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public CASResponse cas(String key, long casId, int expiration, Object value) {
    Span span = helper.buildSpan("cas", key);
    span.setTag("casId", casId);
    span.setTag("value", nullable(value));
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.cas(key, casId, expiration, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <T> OperationFuture<Boolean> add(String key, int expiration, T object,
      Transcoder<T> transcoder) {
    Span span = helper.buildSpan("add", key);
    span.setTag("object", nullable(object));
    span.setTag("expiration", expiration);
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.add(key, expiration, object, transcoder)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Boolean> add(String key, int expiration, Object object) {
    Span span = helper.buildSpan("add", key);
    span.setTag("object", nullable(object));
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.add(key, expiration, object)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> OperationFuture<Boolean> set(String key, int expiration, T object,
      Transcoder<T> transcoder) {
    Span span = helper.buildSpan("set", key);
    span.setTag("object", nullable(object));
    span.setTag("expiration", expiration);
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.set(key, expiration, object, transcoder)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Boolean> set(String key, int expiration, Object object) {
    Span span = helper.buildSpan("set", key);
    span.setTag("expiration", expiration);
    span.setTag("object", nullable(object));
    try (Scope ignore = helper.activate(span)) {
      return super.set(key, expiration, object)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> OperationFuture<Boolean> replace(String key, int expiration, T object,
      Transcoder<T> transcoder) {
    Span span = helper.buildSpan("replace", key);
    span.setTag("object", nullable(object));
    span.setTag("expiration", expiration);
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.replace(key, expiration, object, transcoder)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Boolean> replace(String key, int expiration, Object object) {
    Span span = helper.buildSpan("replace", key);
    span.setTag("object", nullable(object));
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.replace(key, expiration, object)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> GetFuture<T> asyncGet(String key, Transcoder<T> transcoder) {
    Span span = helper.buildSpan("asyncGet", key);
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.asyncGet(key, transcoder)
          .addListener(new TracingGetCompletionListener(span));
    }
  }

  @Override
  public GetFuture<Object> asyncGet(String key) {
    Span span = helper.buildSpan("asyncGet", key);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncGet(key).addListener(new TracingGetCompletionListener(span));
    }
  }

  @Override
  public <T> OperationFuture<CASValue<T>> asyncGets(String key, Transcoder<T> transcoder) {
    Span span = helper.buildSpan("asyncGets", key);
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.asyncGets(key, transcoder)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<CASValue<Object>> asyncGets(String key) {
    Span span = helper.buildSpan("asyncGets", key);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncGets(key).addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> CASValue<T> gets(String key, Transcoder<T> transcoder) {
    Span span = helper.buildSpan("gets", key);
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.gets(key, transcoder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <T> CASValue<T> getAndTouch(String key, int expiration, Transcoder<T> transcoder) {
    Span span = helper.buildSpan("getAndTouch", key);
    span.setTag("expiration", expiration);
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.getAndTouch(key, expiration, transcoder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public CASValue<Object> getAndTouch(String key, int expiration) {
    Span span = helper.buildSpan("getAndTouch", key);
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.getAndTouch(key, expiration);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public CASValue<Object> gets(String key) {
    Span span = helper.buildSpan("gets", key);
    try (Scope ignore = helper.activate(span)) {
      return super.gets(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <T> T get(String key, Transcoder<T> transcoder) {
    Span span = helper.buildSpan("get", key);
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.get(key, transcoder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Object get(String key) {
    Span span = helper.buildSpan("get", key);
    try (Scope ignore = helper.activate(span)) {
      return super.get(key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter,
      Iterator<Transcoder<T>> tcIter) {
    Span span = helper.buildSpan("asyncGetBulk");
    try (Scope ignore = helper.activate(span)) {
      BulkFuture<Map<String, T>> bulkFuture = super.asyncGetBulk(keyIter, tcIter);
      bulkFuture.addListener(new TracingBulkGetCompletionListener(span));
      return bulkFuture;
    }
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
      Iterator<Transcoder<T>> tcIter) {
    Span span = helper.buildSpan("asyncGetBulk");
    span.setTag("keys", TracingHelper.toString(keys));
    try (Scope ignore = helper.activate(span)) {
      BulkFuture<Map<String, T>> bulkFuture = super.asyncGetBulk(keys, tcIter);
      bulkFuture.addListener(new TracingBulkGetCompletionListener(span));
      return bulkFuture;
    }
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter,
      Transcoder<T> transcoder) {
    Span span = helper.buildSpan("asyncGetBulk");
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      BulkFuture<Map<String, T>> bulkFuture = super.asyncGetBulk(keyIter, transcoder);
      bulkFuture.addListener(new TracingBulkGetCompletionListener(span));
      return bulkFuture;
    }
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
      Transcoder<T> transcoder) {
    Span span = helper.buildSpan("asyncGetBulk");
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      BulkFuture<Map<String, T>> bulkFuture = super.asyncGetBulk(keys, transcoder);
      bulkFuture.addListener(new TracingBulkGetCompletionListener(span));
      return bulkFuture;
    }
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(Iterator<String> keyIter) {
    Span span = helper.buildSpan("asyncGetBulk");
    try (Scope ignore = helper.activate(span)) {
      BulkFuture<Map<String, Object>> bulkFuture = super.asyncGetBulk(keyIter);
      bulkFuture.addListener(new TracingBulkGetCompletionListener(span));
      return bulkFuture;
    }
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(Collection<String> keys) {
    Span span = helper.buildSpan("asyncGetBulk");
    span.setTag("keys", TracingHelper.toString(keys));
    try (Scope ignore = helper.activate(span)) {
      BulkFuture<Map<String, Object>> bulkFuture = super.asyncGetBulk(keys);
      bulkFuture.addListener(new TracingBulkGetCompletionListener(span));
      return bulkFuture;
    }
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Transcoder<T> transcoder, String... keys) {
    Span span = helper.buildSpan("asyncGetBulk");
    span.setTag("keys", Arrays.toString(keys));
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      BulkFuture<Map<String, T>> bulkFuture = super.asyncGetBulk(transcoder, keys);
      bulkFuture.addListener(new TracingBulkGetCompletionListener(span));
      return bulkFuture;
    }
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(String... keys) {
    Span span = helper.buildSpan("asyncGetBulk");
    span.setTag("keys", Arrays.toString(keys));
    try (Scope ignore = helper.activate(span)) {
      BulkFuture<Map<String, Object>> bulkFuture = super.asyncGetBulk(keys);
      bulkFuture.addListener(new TracingBulkGetCompletionListener(span));
      return bulkFuture;
    }
  }

  @Override
  public OperationFuture<CASValue<Object>> asyncGetAndTouch(String key, int expiration) {
    Span span = helper.buildSpan("asyncGetAndTouch", key);
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncGetAndTouch(key, expiration)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> OperationFuture<CASValue<T>> asyncGetAndTouch(String key, int expiration,
      Transcoder<T> transcoder) {
    Span span = helper.buildSpan("asyncGetAndTouch", key);
    span.setTag("expiration", expiration);
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.asyncGetAndTouch(key, expiration, transcoder)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public <T> Map<String, T> getBulk(Iterator<String> keyIter, Transcoder<T> transcoder) {
    Span span = helper.buildSpan("getBulk");
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.getBulk(keyIter, transcoder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Map<String, Object> getBulk(Iterator<String> keyIter) {
    Span span = helper.buildSpan("getBulk");
    try (Scope ignore = helper.activate(span)) {
      return super.getBulk(keyIter);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <T> Map<String, T> getBulk(Collection<String> keys, Transcoder<T> transcoder) {
    Span span = helper.buildSpan("getBulk");
    span.setTag("keys", TracingHelper.toString(keys));
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.getBulk(keys, transcoder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Map<String, Object> getBulk(Collection<String> keys) {
    Span span = helper.buildSpan("getBulk");
    span.setTag("keys", TracingHelper.toString(keys));
    try (Scope ignore = helper.activate(span)) {
      return super.getBulk(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <T> Map<String, T> getBulk(Transcoder<T> transcoder, String... keys) {
    Span span = helper.buildSpan("getBulk");
    span.setTag("keys", Arrays.toString(keys));
    span.setTag("transcoder", nullableClass(transcoder));
    try (Scope ignore = helper.activate(span)) {
      return super.getBulk(transcoder, keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Map<String, Object> getBulk(String... keys) {
    Span span = helper.buildSpan("getBulk");
    span.setTag("keys", Arrays.toString(keys));
    try (Scope ignore = helper.activate(span)) {
      return super.getBulk(keys);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Map<SocketAddress, String> getVersions() {
    Span span = helper.buildSpan("getVersions");
    try (Scope ignore = helper.activate(span)) {
      return super.getVersions();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Map<SocketAddress, Map<String, String>> getStats() {
    Span span = helper.buildSpan("getStats");
    try (Scope ignore = helper.activate(span)) {
      return super.getStats();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Map<SocketAddress, Map<String, String>> getStats(String arg) {
    Span span = helper.buildSpan("getStats");
    span.setTag("arg", arg);
    try (Scope ignore = helper.activate(span)) {
      return super.getStats(arg);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long incr(String key, long by) {
    Span span = helper.buildSpan("incr", key);
    span.setTag("by", by);
    try (Scope ignore = helper.activate(span)) {
      return super.incr(key, by);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long incr(String key, int by) {
    Span span = helper.buildSpan("incr", key);
    span.setTag("by", by);
    try (Scope ignore = helper.activate(span)) {
      return super.incr(key, by);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long decr(String key, long by) {
    Span span = helper.buildSpan("decr", key);
    span.setTag("by", by);
    try (Scope ignore = helper.activate(span)) {
      return super.decr(key, by);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long decr(String key, int by) {
    Span span = helper.buildSpan("decr", key);
    span.setTag("by", by);
    try (Scope ignore = helper.activate(span)) {
      return super.decr(key, by);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long incr(String key, long by, long def, int expiration) {
    Span span = helper.buildSpan("incr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.incr(key, by, def, expiration);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long incr(String key, int by, long def, int expiration) {
    Span span = helper.buildSpan("incr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.incr(key, by, def, expiration);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long decr(String key, long by, long def, int expiration) {
    Span span = helper.buildSpan("decr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.decr(key, by, def, expiration);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long decr(String key, int by, long def, int expiration) {
    Span span = helper.buildSpan("decr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.decr(key, by, def, expiration);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public OperationFuture<Long> asyncIncr(String key, long by) {
    Span span = helper.buildSpan("asyncIncr", key);
    span.setTag("by", by);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncIncr(key, by).addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Long> asyncIncr(String key, int by) {
    Span span = helper.buildSpan("asyncIncr", key);
    span.setTag("by", by);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncIncr(key, by).addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Long> asyncDecr(String key, long by) {
    Span span = helper.buildSpan("asyncDecr", key);
    span.setTag("by", by);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncDecr(key, by).addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Long> asyncDecr(String key, int by) {
    Span span = helper.buildSpan("asyncDecr", key);
    span.setTag("by", by);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncDecr(key, by).addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Long> asyncIncr(String key, long by, long def, int expiration) {
    Span span = helper.buildSpan("asyncIncr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncIncr(key, by, def, expiration)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Long> asyncIncr(String key, int by, long def, int expiration) {
    Span span = helper.buildSpan("asyncIncr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncIncr(key, by, def, expiration)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Long> asyncDecr(String key, long by, long def, int expiration) {
    Span span = helper.buildSpan("asyncDecr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncDecr(key, by, def, expiration)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Long> asyncDecr(String key, int by, long def, int expiration) {
    Span span = helper.buildSpan("asyncDecr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    span.setTag("expiration", expiration);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncDecr(key, by, def, expiration)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Long> asyncIncr(String key, long by, long def) {
    Span span = helper.buildSpan("asyncIncr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncIncr(key, by, def)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Long> asyncIncr(String key, int by, long def) {
    Span span = helper.buildSpan("asyncIncr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncIncr(key, by, def)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Long> asyncDecr(String key, long by, long def) {
    Span span = helper.buildSpan("asyncDecr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncDecr(key, by, def)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Long> asyncDecr(String key, int by, long def) {
    Span span = helper.buildSpan("asyncDecr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    try (Scope ignore = helper.activate(span)) {
      return super.asyncDecr(key, by, def)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public long incr(String key, long by, long def) {
    Span span = helper.buildSpan("incr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    try (Scope ignore = helper.activate(span)) {
      return super.incr(key, by, def);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long incr(String key, int by, long def) {
    Span span = helper.buildSpan("incr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    try (Scope ignore = helper.activate(span)) {
      return super.incr(key, by, def);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long decr(String key, long by, long def) {
    Span span = helper.buildSpan("decr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    try (Scope ignore = helper.activate(span)) {
      return super.decr(key, by, def);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public long decr(String key, int by, long def) {
    Span span = helper.buildSpan("decr", key);
    span.setTag("by", by);
    span.setTag("def", def);
    try (Scope ignore = helper.activate(span)) {
      return super.decr(key, by, def);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public OperationFuture<Boolean> delete(String key, int hold) {
    Span span = helper.buildSpan("delete", key);
    span.setTag("hold", hold);
    try (Scope ignore = helper.activate(span)) {
      return super.delete(key, hold)
          .addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Boolean> delete(String key) {
    Span span = helper.buildSpan("delete", key);
    try (Scope ignore = helper.activate(span)) {
      return super.delete(key).addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Boolean> delete(String key, long cas) {
    Span span = helper.buildSpan("delete", key);
    span.setTag("cas", cas);
    try (Scope ignore = helper.activate(span)) {
      return super.delete(key, cas).addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Boolean> flush(int delay) {
    Span span = helper.buildSpan("flush");
    span.setTag("delay", delay);
    try (Scope ignore = helper.activate(span)) {
      return super.flush(delay).addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public OperationFuture<Boolean> flush() {
    Span span = helper.buildSpan("flush");
    try (Scope ignore = helper.activate(span)) {
      return super.flush().addListener(new TracingOperationCompletionListener(span));
    }
  }

  @Override
  public Set<String> listSaslMechanisms() {
    Span span = helper.buildSpan("listSaslMechanisms");
    try (Scope ignore = helper.activate(span)) {
      return super.listSaslMechanisms();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public void shutdown() {
    Span span = helper.buildSpan("shutdown");
    try (Scope ignore = helper.activate(span)) {
      super.shutdown();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public boolean shutdown(long timeout, TimeUnit unit) {
    Span span = helper.buildSpan("shutdown");
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    try (Scope ignore = helper.activate(span)) {
      return super.shutdown(timeout, unit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public boolean waitForQueues(long timeout, TimeUnit unit) {
    Span span = helper.buildSpan("waitForQueues");
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    try (Scope ignore = helper.activate(span)) {
      return super.waitForQueues(timeout, unit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public boolean addObserver(ConnectionObserver obs) {
    return super.addObserver(obs);
  }

  @Override
  public boolean removeObserver(ConnectionObserver obs) {
    return super.removeObserver(obs);
  }

  @Override
  public void connectionEstablished(SocketAddress sa, int reconnectCount) {
    super.connectionEstablished(sa, reconnectCount);
  }

  @Override
  public void connectionLost(SocketAddress sa) {
    super.connectionLost(sa);
  }

  @Override
  public long getOperationTimeout() {
    return super.getOperationTimeout();
  }

  @Override
  public MemcachedConnection getConnection() {
    return super.getConnection();
  }

  @Override
  public TranscodeService getTranscoderService() {
    return super.getTranscoderService();
  }

  @Override
  public ExecutorService getExecutorService() {
    return super.getExecutorService();
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  protected Logger getLogger() {
    return super.getLogger();
  }
}

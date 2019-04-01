/*
 * Copyright 2018-2019 The OpenTracing Authors
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

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.Tags;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import net.spy.memcached.ops.OperationStatus;

class TracingHelper {

  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;
  static final String COMPONENT_NAME = "java-memcached";
  static final String DB_TYPE = "memcached";

  TracingHelper(Tracer tracer, boolean traceWithActiveSpanOnly) {
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
  }

  public Span buildSpan(String operationName) {
    if (traceWithActiveSpanOnly && tracer.activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName).start();
    }
  }

  public Span buildSpan(String operationName, String key) {
    if (traceWithActiveSpanOnly && tracer.activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName).withTag("key", nullable(key)).start();
    }
  }

  private SpanBuilder builder(String operationName) {
    return tracer.buildSpan(operationName)
        .withTag(Tags.COMPONENT.getKey(), COMPONENT_NAME)
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
        .withTag(Tags.DB_TYPE.getKey(), DB_TYPE);
  }

  static void onError(Throwable throwable, Span span) {
    Tags.ERROR.set(span, Boolean.TRUE);

    if (throwable != null) {
      span.log(errorLogs(throwable));
    }
  }

  private static Map<String, Object> errorLogs(Throwable throwable) {
    Map<String, Object> errorLogs = new HashMap<>(2);
    errorLogs.put("event", Tags.ERROR.getKey());
    errorLogs.put("error.object", throwable);
    return errorLogs;
  }

  static String nullable(Object object) {
    return object == null ? "null" : object.toString();
  }

  static String nullableClass(Object object) {
    return object == null ? "null" : object.getClass().getName();
  }

  public static String toString(Collection<?> collection) {
    if (collection == null) {
      return "null";
    }
    boolean first = true;
    StringBuilder builder = new StringBuilder();
    for (Object element : collection) {
      if (first) {
        builder.append(nullable(element));
        first = false;
      } else {
        builder.append(", ").append(nullable(element));
      }
    }

    return builder.toString();
  }

  public Scope activate(Span span) {
    return tracer.scopeManager().activate(span);
  }

  public static void setStatusAndFinish(Span span, OperationStatus status) {
    span.setTag("status.code", nullable(status.getStatusCode()));
    if (status.getMessage() != null) {
      span.setTag("status.message", status.getMessage());
    }
    span.setTag("status.success", status.isSuccess());

    span.finish();
  }
}

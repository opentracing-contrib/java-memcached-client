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

import io.opentracing.Span;
import net.spy.memcached.internal.BulkGetCompletionListener;
import net.spy.memcached.internal.BulkGetFuture;
import net.spy.memcached.ops.OperationStatus;

public class TracingBulkGetCompletionListener implements
    BulkGetCompletionListener {

  private final Span span;

  public TracingBulkGetCompletionListener(Span span) {
    this.span = span;
  }

  @Override
  public void onComplete(BulkGetFuture<?> future) throws Exception {
    OperationStatus status = future.getStatus();
    span.setTag("status.code", nullable(status.getStatusCode()));
    if (status.getMessage() != null) {
      span.setTag("status.message", status.getMessage());
    }
    span.setTag("status.success", status.isSuccess());

    span.finish();
  }
}

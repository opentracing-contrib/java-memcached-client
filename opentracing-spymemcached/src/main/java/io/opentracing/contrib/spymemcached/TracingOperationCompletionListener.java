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

import io.opentracing.Span;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.OperationStatus;

class TracingOperationCompletionListener implements OperationCompletionListener {

  private final Span span;

  TracingOperationCompletionListener(Span span) {
    this.span = span;
  }

  @Override
  public void onComplete(OperationFuture<?> future) {
    OperationStatus status = future.getStatus();
    TracingHelper.setStatusAndFinish(span, status);
  }
}

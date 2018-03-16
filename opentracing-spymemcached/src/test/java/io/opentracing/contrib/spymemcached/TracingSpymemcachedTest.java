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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import net.spy.memcached.MemcachedClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TracingSpymemcachedTest {

  private MockTracer mockTracer = new MockTracer();
  private MemcachedClient client;

  @Before
  public void before() throws IOException {
    mockTracer.reset();
    client = new TracingMemcachedClient(mockTracer, false,
        new InetSocketAddress("localhost", 11211));
  }

  @After
  public void after() {
    if (client != null) {
      client.shutdown();
    }
  }

  @Test
  public void test() throws Exception {

    assertTrue(client.set("key", 0, "value").get());
    Object value = client.get("key");
    assertEquals(value, "value");
    assertTrue(client.touch("key", 1).get());

    await().atMost(15, TimeUnit.SECONDS).until(reportedSpansSize(), equalTo(6));

    List<MockSpan> spans = mockTracer.finishedSpans();
    for (MockSpan span : spans) {
      assertTrue(span.tags().get(Tags.SPAN_KIND.getKey()).equals(Tags.SPAN_KIND_CLIENT));
      assertEquals(TracingHelper.COMPONENT_NAME, span.tags().get(Tags.COMPONENT.getKey()));
      assertEquals("memcached", span.tags().get(Tags.DB_TYPE.getKey()));
      assertEquals(0, span.generatedErrors().size());
    }
  }

  private Callable<Integer> reportedSpansSize() {
    return new Callable<Integer>() {
      @Override
      public Integer call() {
        return mockTracer.finishedSpans().size();
      }
    };
  }
}

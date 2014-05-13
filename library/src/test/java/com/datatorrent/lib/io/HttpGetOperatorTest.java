/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Tests the {@link HttpGetOperator} for sending get requests to specified url
 * and processing responses if output port is connected
 */
public class HttpGetOperatorTest
{
  private final String KEY1 = "key1";
  private final String KEY2 = "key2";

  @Test
  @SuppressWarnings("unchecked")
  public void testGetOperator() throws Exception
  {
    final List<Map<String, String[]>> receivedRequests = new ArrayList<Map<String, String[]>>();
    Handler handler = new AbstractHandler()
    {
      @Override
      public void handle(String string, Request rq, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException
      {
        receivedRequests.add(request.getParameterMap());
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println(request.getParameterNames().nextElement());
        ((Request)request).setHandled(true);
      }

    };

    Server server = new Server(0);
    server.setHandler(handler);
    server.start();

    String url = "http://localhost:" + server.getConnectors()[0].getLocalPort() + "/context";

    TestGetOperator operator = new TestGetOperator();
    operator.setUrl(url);
    operator.setup(null);

    Map<String, String> data = new HashMap<String, String>();
    data.put(KEY1, "1");
    operator.input.process(data);

    data.clear();
    data.put(KEY1, "2");
    data.put(KEY2, "3");
    operator.input.process(data);

    CollectorTestSink sink = new CollectorTestSink();
    operator.output.setSink(sink);

    data.clear();
    data.put("key1", "4");
    operator.input.process(data);

    long startTms = System.currentTimeMillis();
    long waitTime = 10000L;

    while (receivedRequests.size() < 3 && System.currentTimeMillis() - startTms < waitTime) {
      Thread.sleep(250);
    }

    Assert.assertEquals("request count", 3, receivedRequests.size());
    Assert.assertEquals("parameter value", "1", receivedRequests.get(0).get(KEY1)[0]);
    Assert.assertNull("parameter value", receivedRequests.get(0).get(KEY2));
    Assert.assertEquals("parameter value", "2", receivedRequests.get(1).get(KEY1)[0]);
    Assert.assertEquals("parameter value", "3", receivedRequests.get(1).get(KEY2)[0]);
    Assert.assertEquals("parameter value", "4", receivedRequests.get(2).get(KEY1)[0]);

    Assert.assertEquals("emitted size", 1, sink.collectedTuples.size());
    Assert.assertEquals("emitted tuples", KEY1, ((String)sink.collectedTuples.get(0)).trim());
  }

  public class TestGetOperator extends HttpGetOperator<Map<String, String>, String>
  {
    @Override
    protected WebResource getResourceWithQueryParams(Map<String, String> t)
    {
      WebResource wr = wsClient.resource(url);

      for (Entry<String, String> entry : t.entrySet()) {
        wr = wr.queryParam(entry.getKey(), entry.getValue());
      }

      return wr;
    }

    @Override
    protected void processResponse(ClientResponse response)
    {
      output.emit(response.getEntity(String.class));
    }

    private static final long serialVersionUID = 201405121822L;
  }

}

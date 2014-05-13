/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

import java.net.URI;
import java.util.Map;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.ShipContainingJars;

/**
 *
 * Sends tuple as POST with JSON content to the given URL<p>
 * <br>
 * Data of type {@link java.util.Map} is converted to JSON. All other types are sent in their {@link Object#toString()} representation.<br>
 * <br>
 *
 * @param <T>
 * @since 0.3.2
 */
public class HttpPostOutputOperator<T> extends AbstractHttpOperator<T>
{
  @Override
  protected void processTuple(T t)
  {
    if (t instanceof Map) {
      resource.type(MediaType.APPLICATION_JSON).post(new JSONObject((Map<?, ?>)t).toString());
    }
    else {
      resource.post(t.toString());
    }
  }
}
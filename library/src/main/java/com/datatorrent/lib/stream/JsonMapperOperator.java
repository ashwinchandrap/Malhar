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
package com.datatorrent.lib.stream;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * If incoming tuple is JSON String or byte array, the operator converts the tuple to
 * java object and emits, else it converts the incoming object tuple to JSON String and emits.
 *
 * @param <INPUT> input tuple
 * @param <OUTPUT> output tuple
 */
public class JsonMapperOperator<INPUT, OUTPUT> extends BaseOperator
{
  private transient ObjectMapper mapper;
  private transient Class<OUTPUT> outputClass;
  /*
   * fully qualified class name of the output class for converting in case incoming tuple is json string or byte array
   */
  private String outputClassName = "java.util.HashMap"; // default output class
  public final transient DefaultOutputPort<OUTPUT> output = new DefaultOutputPort<OUTPUT>();
  public final transient DefaultInputPort<INPUT> input = new DefaultInputPort<INPUT>()
  {
    @Override
    public void process(INPUT input)
    {
      OUTPUT out;
      if (input instanceof String) {
        out = convertToObject((String)input);
      }
      else if (input instanceof byte[]) {
        out = convertToObject(new String((byte[])input));
      }
      else {
        out = convertToJson(input);
      }
      output.emit(out);
    }

  };

  @SuppressWarnings("unchecked")
  private OUTPUT convertToObject(String input)
  {
    try {
      OUTPUT readValue = mapper.readValue(input, outputClass);
      return readValue;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @SuppressWarnings("unchecked")
  private OUTPUT convertToJson(INPUT input)
  {
    try {
      String jsonString = mapper.writeValueAsString(input);
      return (OUTPUT)jsonString;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setup(OperatorContext context)
  {
    try {
      mapper = new ObjectMapper(new JsonFactory());
      outputClass = (Class<OUTPUT>)Class.forName(outputClassName);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public String getOutputClassName()
  {
    return outputClassName;
  }

  /*
   * Set the fully qualified class name of the object to which json string has to be converted
   */
  public void setOutputClassName(String outputClassName)
  {
    this.outputClassName = outputClassName;
  }

  private static final long serialVersionUID = 201405191115L;
  private static final Logger logger = LoggerFactory.getLogger(JsonMapperOperator.class);
}

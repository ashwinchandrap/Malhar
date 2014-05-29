/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
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
 *
 * @param <INPUT>
 * @param <OUTPUT>
 */
public class JsonMapperOperator<INPUT, OUTPUT> extends BaseOperator
{
  private transient ObjectMapper mapper;
  private transient Class<OUTPUT> outputClass;
  private String outputClassName = "java.util.HashMap"; // default output class
  public final transient DefaultOutputPort<OUTPUT> output = new DefaultOutputPort<OUTPUT>();
  public final transient DefaultInputPort<INPUT> input = new DefaultInputPort<INPUT>()
  {
    @Override
    public void process(INPUT input)
    {
      if (input instanceof String || input instanceof byte[]) {
        convertToObject(input.toString());
      }
      else {
        convertToJson(input);
      }
    }

  };

  @SuppressWarnings("unchecked")
  private void convertToObject(String input)
  {
    try {
      OUTPUT readValue = mapper.readValue(input, outputClass);
      output.emit(readValue);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @SuppressWarnings("unchecked")
  private void convertToJson(INPUT input)
  {
    try {
      String jsonString = mapper.writeValueAsString(input);
      output.emit((OUTPUT)jsonString);
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

  public void setOutputClassName(String outputClassName)
  {
    this.outputClassName = outputClassName;
  }

  private static final long serialVersionUID = 201405191115L;
  private static final Logger logger = LoggerFactory.getLogger(JsonMapperOperator.class);
}

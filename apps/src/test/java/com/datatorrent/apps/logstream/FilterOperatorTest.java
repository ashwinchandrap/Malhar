/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class FilterOperatorTest
{
  public static class TestApplication implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration c)
    {
      LogstreamPropertyRegistry registry = new LogstreamPropertyRegistry();
      registry.bind("LOG_TYPE", "apache");

      TestInputOperator inputOperator = dag.addOperator("InputOperator", new TestInputOperator());
      inputOperator.setRegistry(registry);

      FilterOperator filterOperator = dag.addOperator("FilterOperator", new FilterOperator());
      filterOperator.setRegistry(registry);
      String val = "404";
      filterOperator.addFilterCondition(new String[] {"type=apache", "response", "response.equals(\"404\")"});
      filterOperator.addFilterCondition(new String[] {"type=apache", "default=true"});

      TestOutputOperator outputOperator = dag.addOperator("OutputOperator", new TestOutputOperator());

      dag.addStream("input", inputOperator.outMap, filterOperator.input);
      dag.addStream("filter", filterOperator.outputMap, outputOperator.input);
    }

  }

  public static class TestInputOperator extends BaseOperator implements InputOperator
  {
    private HashMap<String, Object> map;
    private LogstreamPropertyRegistry registry;

    public void setRegistry(LogstreamPropertyRegistry registry)
    {
      this.registry = registry;
    }

    @OutputPortFieldAnnotation(name = "outMap")
    public final transient DefaultOutputPort<HashMap<String, Object>> outMap = new DefaultOutputPort<HashMap<String, Object>>();

    @Override
    public void emitTuples()
    {
      map = new HashMap<String, Object>();
      map.put("LOG_TYPE", registry.getIndex("LOG_TYPE", "apache"));
      String val = new String("404");
      map.put("response", val);
      outMap.emit(map);
    }

  }

  public static class TestOutputOperator extends BaseOperator
  {
    @InputPortFieldAnnotation(name = "input")
    public final transient DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>()
    {
      @Override
      public void process(Map<String, Object> t)
      {
        System.out.println("tuple received ## " + t);
      }

    };
  }

  @Test
  public void testOperator()
  {
    LocalMode.runApp(new TestApplication(), 60000);

    /*
     FilterOperator oper = new FilterOperator();
     LogstreamPropertyRegistry registry = new LogstreamPropertyRegistry();
     registry.bind("LOG_TYPE", "apache");
     oper.setRegistry(registry);
     oper.setup(null);

     //oper.addFilterCondition(new String[]{"type=apache","a","b","c_info","d","a==\"1\"&&b==\"2\"&&c_info==\"abc\""});
     //oper.addFilterCondition(new String[]{"type=apache","d","d==1"});
     //oper.addFilterCondition(new String[]{"type=apache","a","a==\"1\""});
     oper.addFilterCondition(new String[]{"type=apache", "response", "response==\"404\""});
     HashMap<String, Object> inMap = new HashMap<String, Object>();
     inMap.put("LOG_TYPE", registry.getIndex("LOG_TYPE", "apache"));
     //inMap.put("a", "1");
     //inMap.put("b", "2");
     //inMap.put("c_info", "abc");
     //inMap.put("d", 1);
     //inMap.put("e", "3");
     inMap.put("response", "404");

     CollectorTestSink mapSink = new CollectorTestSink();
     oper.outputMap.setSink(mapSink);

     oper.beginWindow(0);
     oper.input.process(inMap);
     oper.endWindow();

     System.out.println(mapSink.collectedTuples);
     //System.out.println("registry = " + registry.toString());
     */

  }

}

/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.HashMap;
import org.junit.Test;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class DimensionOperatorTest
{
  @Test
  public void testOperator()
  {
    DimensionOperator oper = new DimensionOperator();
    LogstreamPropertyRegistry registry = new LogstreamPropertyRegistry();
    registry.bind("LOG_TYPE", "apache");
    registry.bind("FILTER", "ALL");
    registry.bind("FILTER", "ANOTHER_TEST_FILTER");
    oper.setRegistry(registry);
    // user input example::
    // type=apache,timebucket=m,timebucket=h,a:b:c,b:c,b,d,values=x.sum:y.sum:y.avg
    oper.addPropertiesFromString(new String[] {"type=apache", "timebucket=m", "name", "url", "name:url","values=value.sum:value.avg"});

    HashMap<String, Object> inMap1 = new HashMap<String, Object>();
    inMap1.put("LOG_TYPE", registry.getIndex("LOG_TYPE", "apache"));
    inMap1.put("FILTER", registry.getIndex("FILTER", "ALL"));
    inMap1.put("name", "abc");
    inMap1.put("url", "http://www.t.co");
    inMap1.put("value", 25);
    inMap1.put("response", "404");

    HashMap<String, Object> inMap2 = new HashMap<String, Object>();
    inMap2.put("LOG_TYPE", registry.getIndex("LOG_TYPE", "apache"));
    inMap2.put("FILTER", registry.getIndex("FILTER", "ALL"));
    inMap2.put("name", "xyz");
    inMap2.put("url", "http://www.t.co");
    inMap2.put("value", 25);
    inMap2.put("response", "404");

    HashMap<String, Object> inMap3 = new HashMap<String, Object>();
    inMap3.put("LOG_TYPE", registry.getIndex("LOG_TYPE", "apache"));
    inMap3.put("FILTER", registry.getIndex("FILTER", "ALL"));
    inMap3.put("name", "abc");
    inMap3.put("url", "http://www.t.co");
    inMap3.put("value", 25);
    inMap3.put("response", "404");

    HashMap<String, Object> inMap4 = new HashMap<String, Object>();
    inMap4.put("LOG_TYPE", registry.getIndex("LOG_TYPE", "apache"));
    inMap4.put("FILTER", registry.getIndex("FILTER", "ANOTHER_TEST_FILTER"));
    inMap4.put("name", "abc");
    inMap4.put("url", "http://www.t.co");
    inMap4.put("value", 25);
    inMap4.put("response", "404");

    CollectorTestSink mapSink = new CollectorTestSink();
    oper.aggregationsOutput.setSink(mapSink);

    oper.beginWindow(0);
    oper.in.process(inMap1);
    oper.in.process(inMap2);
    oper.in.process(inMap3);
    oper.in.process(inMap4);
    oper.endWindow();

    System.out.println(mapSink.collectedTuples);
    //System.out.println("registry = " + registry.toString());

  }

}

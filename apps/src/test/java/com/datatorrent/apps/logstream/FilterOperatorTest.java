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
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.validation.constraints.AssertTrue;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class FilterOperatorTest
{
  @Test
  @SuppressWarnings("unchecked")
  public void testOperator()
  {
    FilterOperator oper = new FilterOperator();
    LogstreamPropertyRegistry registry = new LogstreamPropertyRegistry();
    registry.bind(LogstreamUtil.LOG_TYPE, "apache");
    oper.setRegistry(registry);
    oper.setup(null);

    String filter1 = "a==\"1\"&&b==\"2\"&&c_info==\"abc\"";
    oper.addFilterCondition(new String[] {"type=apache","a","b","c_info","d", filter1});
    String filter2 = "d==1";
    oper.addFilterCondition(new String[] {"type=apache","d", filter2});
    String filter3 = "a==\"1\"";
    oper.addFilterCondition(new String[] {"type=apache","a", filter3});
    String filter4 = "e==\"2\"";
    oper.addFilterCondition(new String[] {"type=apache","e", filter4});
    String filter5 = "response.equals(\"404\")";
    oper.addFilterCondition(new String[] {"type=apache", "response", filter5});
    String filter6 = "default=true";
    oper.addFilterCondition(new String[] {"type=apache", filter6});
    HashMap<String, Object> inMap = new HashMap<String, Object>();

    inMap.put(LogstreamUtil.LOG_TYPE, registry.getIndex(LogstreamUtil.LOG_TYPE, "apache"));
    inMap.put("a", "1");
    inMap.put("b", "2");
    inMap.put("c_info", "abc");
    inMap.put("d", 1);
    inMap.put("e", "3");
    inMap.put("response", "404");

    Set<Integer> expectedPassSet = new HashSet<Integer>();
    expectedPassSet.add(registry.getIndex(LogstreamUtil.FILTER, filter1));
    expectedPassSet.add(registry.getIndex(LogstreamUtil.FILTER, filter2));
    expectedPassSet.add(registry.getIndex(LogstreamUtil.FILTER, filter3));
    expectedPassSet.add(registry.getIndex(LogstreamUtil.FILTER, filter5));
    expectedPassSet.add(registry.getIndex(LogstreamUtil.FILTER, "apache_DEFAULT"));

    CollectorTestSink mapSink = new CollectorTestSink();
    oper.outputMap.setSink(mapSink);

    oper.beginWindow(0);
    oper.input.process(inMap);
    oper.endWindow();

    Assert.assertEquals("tuple count", 5, mapSink.collectedTuples.size());

    List<HashMap<String, Object>> tuples = mapSink.collectedTuples;

    Set<Integer> actualPassSet = new HashSet<Integer>();
    for (HashMap<String, Object> tuple : tuples) {
      Integer filter = (Integer)tuple.get(LogstreamUtil.FILTER);
      actualPassSet.add(filter);
    }

    Assert.assertEquals("Passed filters", expectedPassSet, actualPassSet);
  }

}

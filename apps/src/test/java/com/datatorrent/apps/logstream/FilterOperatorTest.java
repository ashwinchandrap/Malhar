/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.HashMap;
import org.junit.Test;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class FilterOperatorTest
{
  @Test
  public void testOperator() {

    FilterOperator oper = new FilterOperator();

    oper.addCondition(new String[]{"a","b","c","d","a==\"1\"&&b==\"2\"&&c==\"abc\""});
    oper.addCondition(new String[]{"d","d>0"});
    HashMap<String, Object> inMap = new HashMap<String, Object>();
    inMap.put("a", "1");
    inMap.put("b", "2");
    inMap.put("c", "abc");
    inMap.put("d", 1);
    inMap.put("e", "3");

    CollectorTestSink mapSink = new CollectorTestSink();
    oper.outputMap.setSink(mapSink);

    oper.beginWindow(0);
    oper.input.process(inMap);
    oper.endWindow();

    System.out.println(mapSink.collectedTuples);


  }

}

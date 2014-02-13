/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import com.datatorrent.lib.logs.DimensionObject;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;
import org.junit.Test;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class DimensionOperatorUnifierTest
{

  @Test
  public void testOperator() {
    DimensionOperatorUnifier unifier = new DimensionOperatorUnifier();
    CollectorTestSink sink = new CollectorTestSink();

    unifier.aggregationsOutput.setSink(sink);

    unifier.beginWindow(1);

    Map<String, DimensionObject<String>> tuple1 = new HashMap<String, DimensionObject<String>>();

    tuple1.put("m|201402121900|0|65537|131074|bytes.AVERAGE", new DimensionObject<String>(new MutableDouble(75), "a"));
    tuple1.put("m|201402121900|0|65537|131074|bytes.COUNT", new DimensionObject<String>(new MutableDouble(3.0), "a"));
    tuple1.put("m|201402121900|0|65537|131074|bytes.SUM", new DimensionObject<String>(new MutableDouble(225), "a"));

    Map<String, DimensionObject<String>> tuple2 = new HashMap<String, DimensionObject<String>>();

    tuple2.put("m|201402121900|0|65537|131074|bytes.AVERAGE", new DimensionObject<String>(new MutableDouble(50), "a"));
    tuple2.put("m|201402121900|0|65537|131074|bytes.COUNT", new DimensionObject<String>(new MutableDouble(2.0), "a"));
    tuple2.put("m|201402121900|0|65537|131074|bytes.SUM", new DimensionObject<String>(new MutableDouble(100), "a"));

    Map<String, DimensionObject<String>> tuple3 = new HashMap<String, DimensionObject<String>>();

    tuple3.put("m|201402121900|0|65537|131074|bytes.AVERAGE", new DimensionObject<String>(new MutableDouble(50), "z"));
    tuple3.put("m|201402121900|0|65537|131074|bytes.COUNT", new DimensionObject<String>(new MutableDouble(2.0), "z"));
    tuple3.put("m|201402121900|0|65537|131074|bytes.SUM", new DimensionObject<String>(new MutableDouble(100), "z"));

    Map<String, DimensionObject<String>> tuple4 = new HashMap<String, DimensionObject<String>>();

    tuple4.put("m|201402121900|0|65537|131075|bytes.AVERAGE", new DimensionObject<String>(new MutableDouble(14290.5), "b"));
    tuple4.put("m|201402121900|0|65537|131075|bytes.COUNT", new DimensionObject<String>(new MutableDouble(2.0), "b"));
    tuple4.put("m|201402121900|0|65537|131075|bytes.SUM", new DimensionObject<String>(new MutableDouble(28581.0), "b"));

    Map<String, DimensionObject<String>> tuple5 = new HashMap<String, DimensionObject<String>>();

    tuple5.put("m|201402121900|0|65537|131076|bytes.AVERAGE", new DimensionObject<String>(new MutableDouble(290.75), "c"));
    tuple5.put("m|201402121900|0|65537|131076|bytes.COUNT", new DimensionObject<String>(new MutableDouble(10.0), "c"));
    tuple5.put("m|201402121900|0|65537|131076|bytes.SUM", new DimensionObject<String>(new MutableDouble(8581.0), "c"));

    unifier.process(tuple1);
    unifier.process(tuple2);
    unifier.process(tuple3);
    unifier.process(tuple4);
    unifier.process(tuple5);

    unifier.endWindow();

    System.out.println("## output ##");
    System.out.println(sink.collectedTuples);

  }

}

/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream.PropertyRegistry;

import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * Tests logstream property registry
 */
public class LogstreamPropertyRegistryTest
{
  @Test
  public void test()
  {
    LogstreamPropertyRegistry registry = new LogstreamPropertyRegistry();

    registry.bind("ONE", "a");
    registry.bind("ONE", "b");
    registry.bind("ONE", "c");

    registry.bind("TWO", "x");

    registry.bind("THREE", "1");
    registry.bind("THREE", "2");

    Assert.assertEquals("index 0", registry.getIndex("ONE", "a"), 0);

    Assert.assertEquals("index 0 name", "ONE", registry.lookupName(0));
    Assert.assertEquals("index 0 value", "a", registry.lookupValue(0));

    String[] list = registry.list("ONE");
    Assert.assertEquals("list size", 3, list.length);
    Assert.assertEquals("list value", "a", list[0]);
  }

}

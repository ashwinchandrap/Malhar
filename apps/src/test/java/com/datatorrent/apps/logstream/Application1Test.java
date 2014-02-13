/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import com.datatorrent.api.LocalMode;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class Application1Test
{
  @Test
  public void testSomeMethod() throws Exception
  {
    Configuration conf = new Configuration(false);
    LocalMode lma = LocalMode.newInstance();

    Application1 application = new Application1();

    application.populateDAG(lma.getDAG(), conf);
    lma.cloneDAG();
    lma.getController().run(60000);
  }
}

/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.FSWriter;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.testbench.RandomWordGenerator;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class RecreateApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration c)
  {
    RandomWordGenerator generator = dag.addOperator("words", new RandomWordGenerator());
    Writer writer = dag.addOperator("writer", new Writer());

    generator.setTuplesPerWindow(1000);
    dag.addStream("writewords", generator.output, writer.input);
  }

}


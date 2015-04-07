/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.vertica;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.vertica.Batch;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class TableDataGenerator extends BaseOperator
{
  public final transient DefaultOutputPort<Batch> randomBatchOutput = new DefaultOutputPort<Batch>();
}

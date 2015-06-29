/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.vertica;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

import com.datatorrent.contrib.vertica.Batch;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class HdfsOutputStreamCodec extends KryoSerializableStreamCodec<Batch>
{
  @Override
  public int getPartition(Batch t)
  {
    return t.tableName.hashCode();
  }

}

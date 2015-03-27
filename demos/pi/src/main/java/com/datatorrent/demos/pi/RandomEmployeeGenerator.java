/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class RandomEmployeeGenerator implements InputOperator
{
  StringBuilder sb = new StringBuilder(200);
  long id = 0L;

  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();

  @Override
  public void emitTuples()
  {
    sb.setLength(0);
    sb.append(id++).append(",");
    sb.append("john smith").append(",");
    sb.append("200");

    output.emit(sb.toString().getBytes());
  }

  @Override
  public void beginWindow(long l)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext cntxt)
  {
  }

  @Override
  public void teardown()
  {
  }

}

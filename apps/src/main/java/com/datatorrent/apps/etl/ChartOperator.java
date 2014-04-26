/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.etl;

import java.util.*;

import javax.annotation.Nonnull;

import org.python.google.common.collect.Lists;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;

/**
 *
 * @param <TUPLE>
 * @param <CHART>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public abstract class ChartOperator<TUPLE, CHART> extends BaseOperator
{
  public enum CHART_TYPE
  {
    LINE, LIST, TOP
  }

  @Nonnull
  protected List<CHART> chartParamsList;
  public final transient DefaultInputPort<TUPLE> input = new DefaultInputPort<TUPLE>()
  {
    @Override
    public void process(TUPLE t)
    {
      processTuple(t);
    }

  };

  protected abstract void processTuple(TUPLE t);

  public void setChartParamsList(CHART[] params)
  {
    chartParamsList = Lists.newArrayList(params);
  }

}

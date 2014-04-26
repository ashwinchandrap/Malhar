/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.etl;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.python.google.common.collect.Lists;

import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.apps.etl.MapAggregator.MapAggregateEvent;
import com.datatorrent.apps.etl.XYLineChartMapAggregateOperator.LineChartParams;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class XYLineChartMapAggregateOperator extends ChartOperator<MapAggregateEvent, LineChartParams>
{
  private long currentTimeStamp;
  @Nonnull
  private MapAggregator[] aggregators;
  @Nonnull
  private List<Set<String>> dimensions;
  private Map<Object, Map<Object, Object>> out = new HashMap<Object, Map<Object, Object>>();
  final public transient DefaultOutputPort<Map<Object, Map<Object, Object>>> outputPort = new DefaultOutputPort<Map<Object, Map<Object, Object>>>();
  private transient boolean isFilterTuple;

  @Override
  protected void processTuple(MapAggregateEvent tuple)
  {
    int aggregatorIndex = tuple.getAggregatorIndex();
    Set<String> dimensionKeys = dimensions.get(aggregatorIndex);

    for (LineChartParams lineChartParams : chartParamsList) {
      if (dimensionKeys.equals(lineChartParams.dimensions)) {
        if (lineChartParams.xDimensionKey.equalsIgnoreCase(Constants.TIME_ATTR)) {
          // time series x axis
          long time = (Long)tuple.getDimension(Constants.TIME_ATTR);
          long duration = TimeUnit.MILLISECONDS.convert(lineChartParams.xNumPoints, TimeUnit.valueOf(lineChartParams.xDimensionUnit));
          if (currentTimeStamp - time > duration) {
            continue;
          }
        }
        else {
          // number series x axis
          double xMax = lineChartParams.xNumPoints * Double.parseDouble(lineChartParams.xDimensionUnit);
          double dimension = (Double)tuple.getDimension(lineChartParams.xDimensionKey);
          if (dimension > xMax) {
            continue;
          }
        }

        isFilterTuple = filterTuple(tuple, lineChartParams);
        if (isFilterTuple) {
          genereteOutput(tuple, lineChartParams);
        }

      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentTimeStamp = System.currentTimeMillis();
  }

  @Override
  public void endWindow()
  {
    if (!out.isEmpty()) {
      outputPort.emit(out);
      out.clear();
    }
  }

  public MapAggregator[] getAggregators()
  {
    return aggregators;
  }

  public void setAggregators(MapAggregator[] aggregators)
  {
    this.aggregators = aggregators;
    dimensions = new ArrayList<Set<String>>();
    for (MapAggregator mapAggregator : aggregators) {
      Set<String> dimension = new TreeSet<String>(mapAggregator.getDimensionKeys());
      dimensions.add(dimension);
    }
  }

  private boolean filterTuple(MapAggregateEvent tuple, LineChartParams lineChartParams)
  {
    for (Entry<String, String> entry : lineChartParams.filters.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();
      if (!((String)tuple.getDimension(key)).equalsIgnoreCase(val)) {
        return false;
      }
    }
    return true;
  }

  private void genereteOutput(MapAggregateEvent tuple, LineChartParams lineChartParams)
  {
    int schemaId = chartParamsList.indexOf(lineChartParams);
    HashMap<Object, Object> xy = (HashMap<Object, Object>)out.get(schemaId);

    if (xy == null) {
      xy = new HashMap<Object, Object>();
      out.put(schemaId, xy);
    }

    ArrayList<Object> yList = new ArrayList<Object>();
    for (String metric : lineChartParams.metrics) {
      yList.add(tuple.getMetric(metric));
    }

    xy.put(tuple.getDimension(lineChartParams.xDimensionKey), yList);

  }

  /**
   * Chart parameters corresponding to each line chart
   */
  public static class LineChartParams
  {
    public String name;
    public String type;
    String xDimensionKey; // eg: timestamp
    String xDimensionUnit; // eg: MINUTE
    int xNumPoints; // eg: 15
    @NotNull
    List<String> metrics; // eg: sum, count, avg
    Set<String> dimensions; // eg: bucket, country
    Map<String, String> filters; // eg: bucket = MINUTE, country = US

    public LineChartParams()
    {
    }

    public void init(String[] params)
    {
      //eg: line, timestamp, MINUTE:30, country=US:url=/home, sum:count:avg:avgRespTime
      //eg: line, numUsers, 100:20,  country=US:url=/home, avgRespTime
      dimensions = Sets.newTreeSet();
      filters = Maps.newHashMap();

      // chart type
      type = params[0];

      // x axis
      xDimensionKey = params[1];
      dimensions.add(xDimensionKey);
      if (xDimensionKey.equalsIgnoreCase(Constants.TIME_ATTR)) {
        dimensions.add("bucket");
      }
      String[] x = params[2].split(":");
      xDimensionUnit = x[0];
      xNumPoints = Integer.parseInt(x[1]);

      // filter
      if (params[3] != null && !params[3].isEmpty()) {
        String[] filerList = params[3].split(":");
        for (String elem : filerList) {
          String[] fil = elem.split("=");
          filters.put(fil[0], fil[1]);
          dimensions.add(fil[0]);
        }
      }

      // metrics
      String[] metricList = params[4].split(":");
      metrics = Lists.newArrayList(metricList);
    }

    @Override
    public String toString()
    {
      return "LineChartParams{" + "name=" + name + ", type=" + type + ", xDimensionKey=" + xDimensionKey + ", xDimensionUnit=" + xDimensionUnit + ", xNumPoints=" + xNumPoints + ", metrics=" + metrics + ", dimensions=" + dimensions + ", filters=" + filters + '}';
    }

  }

}

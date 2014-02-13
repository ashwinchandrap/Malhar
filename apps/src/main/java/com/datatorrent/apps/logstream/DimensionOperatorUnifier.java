/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.apps.logstream.Util.AggregateOperation;
import com.datatorrent.lib.logs.DimensionObject;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang.mutable.MutableDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class DimensionOperatorUnifier implements Unifier<Map<String, DimensionObject<String>>>
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionOperatorUnifier.class);
  @OutputPortFieldAnnotation(name = "aggregationsOutput")
  public final transient DefaultOutputPort<Map<String, DimensionObject<String>>> aggregationsOutput = new DefaultOutputPort<Map<String, DimensionObject<String>>>();
  Map<String, DimensionObject<String>> unifiedOutput;
  private Map<String, Map<String, Map<AggregateOperation, Number>>> cacheObject = new HashMap<String, Map<String, Map<AggregateOperation, Number>>>();
  private transient boolean firstTuple = true;
  private HashMap<String, Number> recordType = new HashMap<String, Number>();

  @Override
  public void process(Map<String, DimensionObject<String>> tuple)
  {
    if (firstTuple) {
      DimensionOperatorUnifier.this.extractType(tuple);
      firstTuple = false;
    }

    // tuple will have one record each per operation type. currently 3 record one for each of SUM, COUNT, AVERAGE
    Iterator<Entry<String, DimensionObject<String>>> iterator = tuple.entrySet().iterator();
    String randomKey = null;
    String key = null;

    if (iterator.hasNext()) {
      randomKey = iterator.next().getKey();
      String[] split = randomKey.split("\\.");
      key = split[0];
    }

    String[] split = randomKey.split("|");
    Number receivedFilter = new Integer(split[3]);
    Number expectedFilter = recordType.get("FILTER");

    if (!receivedFilter.equals(expectedFilter)) {
      logger.error("Unexpected tuple");
      logger.error("expected filter = {} received = {}", expectedFilter, receivedFilter);
    }

    DimensionOperatorUnifier.this.computeAddition(tuple, AggregateOperation.SUM, key);
    DimensionOperatorUnifier.this.computeAddition(tuple, AggregateOperation.COUNT, key);
    DimensionOperatorUnifier.this.computeAverage(tuple, AggregateOperation.AVERAGE, key);

    /*
     for (Entry<String, Map<String, Map<AggregateOperation, Number>>> keys : tuple.entrySet()) {
     String key = keys.getKey();
     Map<String, Map<AggregateOperation, Number>> dimValues = keys.getValue();
     Map<String, Map<AggregateOperation, Number>> cacheDimValues;
     if ((cacheDimValues = cacheObject.get(key)) != null) {
     // do computations
     for (Entry<String, Map<AggregateOperation, Number>> dimValue : dimValues.entrySet()) {
     String dimValueName = dimValue.getKey();
     Map<AggregateOperation, Number> operations = dimValue.getValue();
     Map<AggregateOperation, Number> cacheOperations;
     if ((cacheOperations = cacheDimValues.get(dimValueName)) != null) {
     // do computations
     double sum;
     if ((sum = operations.get(AggregateOperation.SUM).doubleValue()) > 0) {
     MutableDouble aggrVal = (MutableDouble)cacheOperations.get(AggregateOperation.SUM);
     aggrVal.add(sum);
     }

     double count;
     if ((count = operations.get(AggregateOperation.COUNT).doubleValue()) > 0) {
     MutableDouble aggrVal = (MutableDouble)cacheOperations.get(AggregateOperation.COUNT);
     aggrVal.add(count);
     }

     if (count > 0) {
     double avg = operations.get(AggregateOperation.AVERAGE).doubleValue();
     double cacheAvg = cacheOperations.get(AggregateOperation.AVERAGE).doubleValue();
     double cacheCount = cacheOperations.get(AggregateOperation.COUNT).doubleValue(); // final value calculated in above count calculation

     double newAvg = (avg * count + cacheAvg * (cacheCount - count))/cacheCount;
     cacheOperations.put(AggregateOperation.AVERAGE, new MutableDouble(newAvg));
     }
     }
     else {
     // copy the aggregation operations
     cacheDimValues.put(dimValueName, operations);
     }
     }
     }
     else {
     // copy the valueNames and their aggregations
     cacheObject.put(key, dimValues);
     }
     }
     */
  }

  private void computeAddition(Map<String, DimensionObject<String>> tuple, AggregateOperation opType, String key)
  {
    String finalKey = key + "." + opType.name();
    if (tuple.containsKey(finalKey)) {

      DimensionObject<String> dimObj = tuple.get(finalKey);
      if (cacheObject.containsKey(key)) {
        Map<String, Map<AggregateOperation, Number>> cacheAggrs = cacheObject.get(key);
        if (cacheAggrs.containsKey(dimObj.getVal())) {
          Map<AggregateOperation, Number> cacheAggr = cacheAggrs.get(dimObj.getVal());
          if (cacheAggr.containsKey(opType)) {
            double cacheVal = cacheAggr.get(opType).doubleValue();
            double newVal = dimObj.getCount().doubleValue();
            double finalVal = cacheVal + newVal;

            cacheAggr.put(opType, finalVal);
          }
          else {
            cacheAggr.put(opType, dimObj.getCount().doubleValue());
          }
        }
        else {
          Map<AggregateOperation, Number> newAggrs = new EnumMap<AggregateOperation, Number>(AggregateOperation.class);
          newAggrs.put(opType, dimObj.getCount().doubleValue());
          cacheAggrs.put(dimObj.getVal(), newAggrs);
        }
      }
      else {
        Map<AggregateOperation, Number> newAggrs = new EnumMap<AggregateOperation, Number>(AggregateOperation.class);
        Map<String, Map<AggregateOperation, Number>> cacheAggrs = new HashMap<String, Map<AggregateOperation, Number>>();

        newAggrs.put(opType, dimObj.getCount().doubleValue());
        cacheAggrs.put(dimObj.getVal(), newAggrs);
        cacheObject.put(key, cacheAggrs);
      }
    }

  }

  private void computeAverage(Map<String, DimensionObject<String>> tuple, AggregateOperation opType, String key)
  {
    String finalKey = key + "." + opType.name();
    if (tuple.containsKey(finalKey)) {

      DimensionObject<String> dimObj = tuple.get(finalKey);
      if (cacheObject.containsKey(key)) {
        Map<String, Map<AggregateOperation, Number>> cacheAggrs = cacheObject.get(key);
        if (cacheAggrs.containsKey(dimObj.getVal())) {
          Map<AggregateOperation, Number> cacheAggr = cacheAggrs.get(dimObj.getVal());
          if (cacheAggr.containsKey(opType)) {
            double cacheAvg = cacheAggr.get(opType).doubleValue();
            double cacheCount = cacheAggr.get(AggregateOperation.COUNT).doubleValue(); // this is total count, should be computed before calling average

            double newAvg = dimObj.getCount().doubleValue();
            double newCount = tuple.get(key + "." + AggregateOperation.COUNT.name()).getCount().doubleValue();

            double finalVal = (cacheAvg * (cacheCount - newCount) + newAvg * newCount) / cacheCount;

            //logger.info("AVEREGE.. cacheAvg = {} cacheCount = {} newAvg = {} newCount = {} finalAvg = {}", cacheAvg, cacheCount, newAvg, newCount, finalVal);
            cacheAggr.put(opType, finalVal);
          }
          else {
            cacheAggr.put(opType, dimObj.getCount().doubleValue());
          }
        }
        else {
          Map<AggregateOperation, Number> newAggrs = new EnumMap<AggregateOperation, Number>(AggregateOperation.class);
          newAggrs.put(opType, dimObj.getCount().doubleValue());
          cacheAggrs.put(dimObj.getVal(), newAggrs);
        }
      }
      else {
        Map<AggregateOperation, Number> newAggrs = new EnumMap<AggregateOperation, Number>(AggregateOperation.class);
        Map<String, Map<AggregateOperation, Number>> cacheAggrs = new HashMap<String, Map<AggregateOperation, Number>>();

        newAggrs.put(opType, dimObj.getCount().doubleValue());
        cacheAggrs.put(dimObj.getVal(), newAggrs);
        cacheObject.put(key, cacheAggrs);
      }
    }

  }

  @Override
  public void beginWindow(long l)
  {
    //unifiedOutput = new HashMap<String, DimensionObject<String>>();
    cacheObject = new HashMap<String, Map<String, Map<AggregateOperation, Number>>>();
  }

  @Override
  public void endWindow()
  {
    //logger.info("in end window, cache object size = {}", cacheObject.size());
    Map<String, DimensionObject<String>> outputAggregationsObject;

    for (Entry<String, Map<String, Map<AggregateOperation, Number>>> keys : cacheObject.entrySet()) {
      String key = keys.getKey();
      Map<String, Map<AggregateOperation, Number>> dimValues = keys.getValue();

      for (Entry<String, Map<AggregateOperation, Number>> dimValue : dimValues.entrySet()) {
        String dimValueName = dimValue.getKey();
        Map<AggregateOperation, Number> operations = dimValue.getValue();

        outputAggregationsObject = new HashMap<String, DimensionObject<String>>();

        for (Entry<AggregateOperation, Number> operation : operations.entrySet()) {
          AggregateOperation aggrOperationType = operation.getKey();
          Number aggr = operation.getValue();

          String outKey = key + "." + aggrOperationType.name();
          DimensionObject<String> outDimObj = new DimensionObject<String>(new MutableDouble(aggr), dimValueName);

          outputAggregationsObject.put(outKey, outDimObj);

        }
        //logger.info("emitting tuple {}", outputAggregationsObject);
        aggregationsOutput.emit(outputAggregationsObject);
      }

    }
  }

  @Override
  public void setup(OperatorContext t1)
  {
  }

  @Override
  public void teardown()
  {
  }

  private void extractType(Map<String, DimensionObject<String>> tuple)
  {
    Iterator<Entry<String, DimensionObject<String>>> iterator = tuple.entrySet().iterator();
    String randomKey = null;

    if (iterator.hasNext()) {
      randomKey = iterator.next().getKey();
    }
    String[] split = randomKey.split("|");
    Number filterId = new Integer(split[3]);

    recordType.put("FILTER", filterId);
  }

}

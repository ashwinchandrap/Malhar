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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang.mutable.MutableDouble;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class DimensionOperatorUnifier implements Unifier<Map<String, Map<String, Map<AggregateOperation, Number>>>>
{
  @OutputPortFieldAnnotation(name = "aggregationsOutput")
  public final transient DefaultOutputPort<Map<String, DimensionObject<String>>> aggregationsOutput = new DefaultOutputPort<Map<String, DimensionObject<String>>>();
  Map<String, DimensionObject<String>> unifiedOutput;
  private Map<String, Map<String, Map<AggregateOperation, Number>>> cacheObject;

  @Override
  public void process(Map<String, Map<String, Map<AggregateOperation, Number>>> tuple)
  {
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

          String outKey = key + aggrOperationType.name();
          DimensionObject<String> outDimObj = new DimensionObject<String>((MutableDouble)aggr, dimValueName);

          outputAggregationsObject.put(outKey, outDimObj);

        }
        aggregationsOutput.emit(outputAggregationsObject);
      }

    }
  }

  @Override
  public void setup(OperatorContext t1)
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void teardown()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}

/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
import com.datatorrent.api.*;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codehaus.janino.CompileException;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Parser.ParseException;
import org.codehaus.janino.Scanner.ScanException;

import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.apps.logstream.PropertyRegistry.PropertyRegistry;
import com.datatorrent.lib.util.KryoSerializableStreamCodec;
import com.google.common.collect.Sets;
import java.util.*;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class FilterOperator extends BaseOperator implements Partitionable<FilterOperator>
{
  /**
   * key: condition expression
   * value: list of keys on which the condition is
   */
  private transient HashMap<String, String[]> conditions = new HashMap<String, String[]>();
  private transient HashMap<String, Object> filterTuple;
  @InputPortFieldAnnotation(name = "input")
  private transient static PropertyRegistry<String> registry;

  public void setRegistry(PropertyRegistry<String> registry)
  {
    this.registry = registry;
  }
  public final transient DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> map)
    {
      filterTuple = new HashMap<String, Object>(map);
      ArrayList<String> filterList = new ArrayList<String>();
      String[] filterArray = new String[conditions.size()];

      for (String condition : conditions.keySet()) {
        if (evaluate(condition, map)) {
          filterList.add(condition);
        }
      }
      filterTuple.put("FILTER_LIST", filterList);
      outputMap.emit(filterTuple);
    }

    /**
     * Stream codec used for partitioning.
     */
    @Override
    public Class<? extends StreamCodec<Map<String, Object>>> getStreamCodec()
    {
      return FilterOperatorStreamCodec.class;
    }

  };

  private boolean evaluate(String condition, Map<String, Object> map)
  {
    boolean ret = false;

    String[] keys = conditions.get(condition);
    Object[] values = new Object[keys.length];
    Class[] types = new Class[keys.length];

    try {
      for (int i = 0; i < keys.length; i++) {
        values[i] = map.get(keys[i]);
        types[i] = map.get(keys[i]).getClass();
      }

      ExpressionEvaluator ee = new ExpressionEvaluator(condition, boolean.class, keys, types);
      ret = (Boolean)ee.evaluate(values);

    }
    catch (CompileException ex) {
      Logger.getLogger(FilterOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
    catch (ParseException ex) {
      Logger.getLogger(FilterOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
    catch (ScanException ex) {
      Logger.getLogger(FilterOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
    catch (InvocationTargetException ex) {
      Logger.getLogger(FilterOperator.class.getName()).log(Level.SEVERE, null, ex);
    }

    return ret;

  }

  public void addCondition(String[] condition)
  {
    // list of keys followed by the expressions, all entities separated by semi colon
    //a,b,c,a==1&&b==2&&c=="abc"
    //String[] split = condition.split(";");

    String[] keys = new String[condition.length - 1];
    System.arraycopy(condition, 0, keys, 0, keys.length);

    String expression = condition[condition.length - 1];

    conditions.put(expression, keys);
  }

  //@OutputPortFieldAnnotation(name = "outputFilterMap")
  //public final transient DefaultOutputPort<HashMap<String, Map<String, Object>>> outputFilterMap = new DefaultOutputPort<HashMap<String, Map<String, Object>>>();

  @OutputPortFieldAnnotation(name = "outputMap")
  public final transient DefaultOutputPort<HashMap<String, Object>> outputMap = new DefaultOutputPort<HashMap<String, Object>>();

  @Override
  public Collection<Partition<FilterOperator>> definePartitions(Collection<Partition<FilterOperator>> partitions, int incrementalCapacity)
  {
    ArrayList<Partition<FilterOperator>> newPartitions = new ArrayList<Partition<FilterOperator>>();
    //int partitionSize = LOG_TYPE.values().length;
    String[] logTypes = registry.list("LOG_TYPE");
    int partitionSize = logTypes.length;
    int expectedPartitionSize = partitions.size() + incrementalCapacity;
    if (expectedPartitionSize > partitionSize) {
      partitionSize = expectedPartitionSize;
    }

    for (int i = 0; i < partitionSize; i++) {
      FilterOperator filterOperator = new FilterOperator();

      Partition<FilterOperator> partition = new DefaultPartition<FilterOperator>(filterOperator);
      newPartitions.add(partition);
    }

    /*
    int partitionBits = (Integer.numberOfLeadingZeros(0) - Integer.numberOfLeadingZeros(partitionSize - 1));

    int partitionMask = 0;
    if (partitionBits > 0) {
      partitionMask = -1 >>> (Integer.numberOfLeadingZeros(-1)) - partitionBits;
    }
    */
    int partitionMask = -1; // all bits

    for (int i=0; i <= newPartitions.size(); i++) {
      Partition<FilterOperator> partition = newPartitions.get(i);
      String partitionVal = logTypes[i % logTypes.length];
      int key = registry.getIndex("LOG_TYPE", partitionVal);
      partition.getPartitionKeys().put(input, new PartitionKeys(partitionMask, Sets.newHashSet(key)));
    }

    return newPartitions;

  }

  /* Default repartitioning should mostly be enough for repartitioning
  @Override
  public Response processStats(BatchedOperatorStats bos)
  {
    Response res = new Response();

    //TODO: set res.repartitionRequired to true based on dynamic partitioning logic
    // so that it calls definePartitions when its true

    return res;

  }
  */

  public static class FilterOperatorStreamCodec extends KryoSerializableStreamCodec<Map<String, Object>>
  {
    @Override
    public int getPartition(Map<String, Object> o)
    {
      /* LOGIC 1
      int ret = 0;
      //LOG_TYPE type = LOG_TYPE.valueOf(o.getClass().getSimpleName());
      LOG_TYPE type = (LOG_TYPE)o.get("LOG_TYPE");

      switch (type) {
        case APACHE:
          ret = 0;
          break;
        case MYSQL:
          ret = 1;
          break;
        case SYS:
          ret = 2;
          break;
        default: // APACHE
          ret = 0;
      }

      return ret;
      */

      int ret;
      String[] list = registry.list("LOG_TYPE");
      if (list == null) {
        return 0;
      } else if (list.length == 0) {
        return 0;
      }

      ret = registry.getIndex("LOG_TYPE", (String)o.get("LOG_TYPE"));
      return ret;

    }
  }

  /*
  enum LOG_TYPE {
    APACHE, MYSQL, SYS
  }
  */
}

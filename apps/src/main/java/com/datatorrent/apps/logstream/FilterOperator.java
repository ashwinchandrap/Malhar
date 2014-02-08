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
import com.datatorrent.api.Context.OperatorContext;
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
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class FilterOperator extends BaseOperator
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(FilterOperator.class);
  /**
   * key: type
   * value --> map of
   * key: condition expression
   * value: list of keys on which the condition is
   */
  private HashMap<Integer, Map<String, String[]>> conditionList = new HashMap<Integer, Map<String, String[]>>();
  private transient HashMap<String, ExpressionEvaluator> evaluators = new HashMap<String, ExpressionEvaluator>();
  private static PropertyRegistry<String> registry;

  public void setRegistry(PropertyRegistry<String> registry)
  {
    FilterOperator.this.registry = registry;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    registry.bind("FILTER", "NO_CONDITION"); // used to represent the output tuple on which no condition is applied
  }

  @InputPortFieldAnnotation(name = "input")
  public final transient DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> map)
    {
      HashMap<String, Object> filterTuple;
      int typeId = (Integer)map.get("LOG_TYPE");
      Object resp = map.get("response");
      Map<String, String[]> conditions = conditionList.get(typeId);
      if (conditions != null) {
        for (String condition : conditions.keySet()) {
          if (evaluate(condition, map, conditions.get(condition))) {
            int index = registry.getIndex("FILTER", condition);
            filterTuple = new HashMap<String, Object>(map);
            filterTuple.put("FILTER", index);
            outputMap.emit(filterTuple);
          }
        }
      }

      // emit the same tuple once without any condition
      int index = registry.getIndex("FILTER", "NO_CONDITION");
      map.put("FILTER", index);
      outputMap.emit((HashMap<String, Object>)map);
    }

    private boolean evaluate(String condition, Map<String, Object> map, String[] keys)
    {
      boolean ret = false;

      //String[] keys = conditions.get(condition);
      Object[] values = new Object[keys.length];
      Class[] classTypes = new Class[keys.length];
      ExpressionEvaluator ee = evaluators.get(condition);

      try {
        for (int i = 0; i < keys.length; i++) {
          Object val = map.get(keys[i]);
          //Object val = new String("404");
        /*
           if (((String)val).compareTo("404") == 0) {
           logger.info("EQUALSSSSS");
           }
           char[] valCharArr = val.toString().toCharArray();
           logger.info("value is {} and length is {} bytes are {}", val, val.toString().length(), val.toString().getBytes());
           for (Character elem : valCharArr) {
           System.out.print(elem + " | ");

           }
           */
          if (val == null) {
            logger.info("filter key {} missing in input record {}", keys[i], map);
            return ret;
          }
          else {
            values[i] = val;
            classTypes[i] = val.getClass();
          }
        }

        if (ee == null) {
          ee = new ExpressionEvaluator(condition, Boolean.class, keys, classTypes);
          evaluators.put(condition, ee);
        }



        //ee = new ExpressionEvaluator(condition, Boolean.class, keys, classTypes);
        //ExpressionEvaluator ee = new ExpressionEvaluator(condition, Boolean.class, keys, classTypes);
        ret = (Boolean)ee.evaluate(values);
        //Object evaluate = ee.evaluate(values);
        logger.debug("expression evaluated to {} for expression: {} with key class types: {} keys: {} values: {}", ret, condition, Arrays.toString(classTypes), Arrays.toString(keys), Arrays.toString(values));

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

    /**
     * Stream codec used for partitioning.
     *
     * @Override
     * public Class<? extends StreamCodec<Map<String, Object>>> getStreamCodec()
     * {
     * return FilterOperatorStreamCodec.class;
     * }
     */
  };

  public void addFilterCondition(String[] condition)
  {
    // list of keys followed by the expressions, all entities separated by semi colon
    //type=apache,a,b,c,a==1&&b==2&&c=="abc"
    //String[] split = condition.split(";");

    String[] split = condition[0].split("=");
    String type = split[1];
    int typeId = registry.getIndex("LOG_TYPE", type);
    String[] keys = new String[condition.length - 2];

    System.arraycopy(condition, 1, keys, 0, keys.length);

    for (String string : keys) {
      System.out.println("condition keys = " + string);
    }
    String expression = condition[condition.length - 1];

    Map<String, String[]> conditions = conditionList.get(typeId);
    if (conditions == null) {
      conditions = new HashMap<String, String[]>();
      conditionList.put(typeId, conditions);
    }

    conditions.put(expression, keys);
    if (registry != null) {
      registry.bind("FILTER", expression);
    }
  }

  //@OutputPortFieldAnnotation(name = "outputFilterMap")
  //public final transient DefaultOutputPort<HashMap<String, Map<String, Object>>> outputFilterMap = new DefaultOutputPort<HashMap<String, Map<String, Object>>>();
  @OutputPortFieldAnnotation(name = "outputMap")
  public final transient DefaultOutputPort<HashMap<String, Object>> outputMap = new DefaultOutputPort<HashMap<String, Object>>();

  /*
   @Override
   public Collection<Partition<FilterOperator>> definePartitions(Collection<Partition<FilterOperator>> partitions, int incrementalCapacity)
   {
   ArrayList<Partition<FilterOperator>> newPartitions = new ArrayList<Partition<FilterOperator>>();
   //int partitionSize = LOG_TYPE.values().length;
   String[] logTypes = registry.list("LOG_TYPE");
   int partitionSize = logTypes.length;l
   int expectedPartitionSize;
   if (partitions.size() == 1) {
   // initial partitioning
   expectedPartitionSize = partitionSize;
   } else {
   // TODO: figure out what to do in this scenerio
   //expectedPartitionSize = partitionSize * 2;
   expectedPartitionSize = partitionSize;
   }

   for (int i = 0; i < expectedPartitionSize; i++) {
   FilterOperator filterOperator = new FilterOperator();

   Partition<FilterOperator> partition = new DefaultPartition<FilterOperator>(filterOperator);
   newPartitions.add(partition);
   }

   int partitionMask = -1; // all bits

   for (int i=0; i <= newPartitions.size(); i++) {
   Partition<FilterOperator> partition = newPartitions.get(i);
   String partitionVal = logTypes[i % logTypes.length];
   int key = registry.getIndex("LOG_TYPE", partitionVal);
   partition.getPartitionKeys().put(input, new PartitionKeys(partitionMask, Sets.newHashSet(key)));
   }

   return newPartitions;

   }
   */

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

  /*
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
   */

  /*
   enum LOG_TYPE {
   APACHE, MYSQL, SYS
   }
   */
}

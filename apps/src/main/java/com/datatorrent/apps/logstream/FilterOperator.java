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
import java.util.*;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.janino.ExpressionEvaluator;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.annotation.ShipContainingJars;

import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.apps.logstream.PropertyRegistry.PropertyRegistry;
import com.datatorrent.common.util.DTThrowable;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
@ShipContainingJars(classes = {org.codehaus.janino.ExpressionEvaluator.class})
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
  private PropertyRegistry<String> registry;

  public void setRegistry(PropertyRegistry<String> registry)
  {
    FilterOperator.this.registry = registry;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    LogstreamPropertyRegistry.setInstance(registry);
  }

  @InputPortFieldAnnotation(name = "input")
  public final transient DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> map)
    {
      HashMap<String, Object> filterTuple;
      int typeId = (Integer)map.get(LogstreamUtil.LOG_TYPE);
      Map<String, String[]> conditions = conditionList.get(typeId);
      if (conditions != null) {
        for (String condition : conditions.keySet()) {
          if (evaluate(condition, map, conditions.get(condition))) {
            int index = registry.getIndex(LogstreamUtil.FILTER, condition);
            filterTuple = new HashMap<String, Object>(map);
            filterTuple.put(LogstreamUtil.FILTER, index);
            outputMap.emit(filterTuple);
          }
        }
      }

      // emit the same tuple for default condition
      int defaultFilterIndex = registry.getIndex(LogstreamUtil.FILTER, registry.lookupValue(typeId) + "_" + "DEFAULT");

      if (defaultFilterIndex >= 0) {
        map.put(LogstreamUtil.FILTER, defaultFilterIndex);
        outputMap.emit((HashMap<String, Object>)map);
      }
    }

    private boolean evaluate(String condition, Map<String, Object> map, String[] keys)
    {
      boolean ret = false;

      Object[] values = new Object[keys.length];
      Class[] classTypes = new Class[keys.length];
      ExpressionEvaluator ee = evaluators.get(condition);

      try {
        for (int i = 0; i < keys.length; i++) {
          Object val = map.get(keys[i]);

          if (val == null) {
            logger.debug("filter key {} missing in input record {}", keys[i], map);
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

        ret = (Boolean)ee.evaluate(values);
        logger.debug("expression evaluated to {} for expression: {} with key class types: {} keys: {} values: {}", ret, condition, Arrays.toString(classTypes), Arrays.toString(keys), Arrays.toString(values));

      }
      catch (Throwable t) {
        DTThrowable.rethrow(t);
      }

      return ret;
    }

  };

  public void addFilterCondition(String[] condition)
  {
    // TODO: validations
    if (condition.length == 2) {
      logger.info(Arrays.toString(condition));
      String[] split = condition[0].split("=");
      String type = split[1];
      String[] split1 = condition[1].split("=");
      if (split1[1].toLowerCase().equals("true")) {
        registry.bind(LogstreamUtil.FILTER, type + "_" + "DEFAULT");
      }

    }
    else if (condition.length == 3) {
      String[] split = condition[0].split("=");
      String type = split[1];
      int typeId = registry.getIndex(LogstreamUtil.LOG_TYPE, type);
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
        registry.bind(LogstreamUtil.FILTER, expression);
      }
    }
    else {
      //THROW ERROR
    }
  }

  @OutputPortFieldAnnotation(name = "outputMap")
  public final transient DefaultOutputPort<HashMap<String, Object>> outputMap = new DefaultOutputPort<HashMap<String, Object>>();
}

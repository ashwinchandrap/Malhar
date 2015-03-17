/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.log4j.Level;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.DTThrowable;
import java.lang.reflect.InvocationTargetException;
import java.lang.Boolean;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class JaninoRulesOperator extends BaseOperator
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  private transient ExpressionEvaluator ee;
  private String expression;
  private String[] parameterNames;
  private Class[] parameterTypes;
  private Class returnType;
  private Range range;

  @Override
  public void setup(OperatorContext context)
  {
    createExpression();
  }

  private void createExpression()
  {
    try {
      ee = new ExpressionEvaluator(
              expression, // expression
              returnType, // expressionType
              parameterNames, // parameterNames
              parameterTypes // parameterTypes
              );
    }
    catch (CompileException ex) {
      logger.error("uh oh!! ", ex);
      DTThrowable.rethrow(ex);
    }

  }

  public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer tuple)
    {
      try {
        Object[] vals = getParameterVals(tuple);
        boolean result = (Boolean)ee.evaluate(vals);
        output.emit(String.valueOf(result));
      }
      catch (InvocationTargetException ex) {
        logger.error("uh oh! while process:", ex);
      }
    }

  };

  private Object[] getParameterVals(Integer tuple)
  {
    Object[] vals = new Object[6];
    vals[0] = tuple;
    vals[1] = 1;
    vals[2] = 2;
    vals[3] = 3;
    vals[4] = 40;
    vals[5] = range;
    return vals;
  }

  public String getExpression()
  {
    return expression;
  }

  public void setExpression(String expression)
  {
    logger.info("setting expression :: ", expression);
    this.expression = expression;
    createExpression();
  }

  public void setParameterNames(String names)
  {
    parameterNames = names.split(":");
  }

  public void setParameterTypes(String types)
  {
    String[] parTypes = types.split(":");
    parameterTypes = new Class[parTypes.length];
    for (int i = 0; i < parTypes.length; i++) {
      parameterTypes[i] = TypeFinder.getType(parTypes[i]);
    }
  }

  public void setReturnType(String type)
  {
    returnType = TypeFinder.getType(type);
  }

  public void setRange(Range range) {
    this.range = range;
  }

  public Range getRange() {
    return range;
  }

  public static class Range
  {
    private int[] rangeArr;

    public void setRange(String rangeStr)
    {
      String[] split = rangeStr.split(":");
      rangeArr = new int[split.length];
      for (int i = 0; i < split.length; i++) {
        rangeArr[i] = Integer.parseInt(split[i]);
      }
    }

    public boolean contains(int a)
    {
      for (int i : rangeArr) {
        if (a == i) {
          return true;
        }
      }

      return false;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(JaninoRulesOperator.class);
}

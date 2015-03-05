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

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class RulesOperator extends BaseOperator
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  ExpressionEvaluator ee;

  @Override
  public void setup(OperatorContext context)
  {
    try {
      ee = new ExpressionEvaluator(
              "c > d ? c : d", // expression
              int.class, // expressionType
              new String[] {"c", "d"}, // parameterNames
              new Class[] {int.class, int.class} // parameterTypes
              );
    }
    catch (CompileException ex) {
      logger.error("uh oh!! ",ex);
      DTThrowable.rethrow(ex);
    }
  }

  public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer tuple)
    {
      try {
        Object[] vals = new Object[2];
        vals[0] = tuple;
        vals[1] = 50;
        int result = (Integer)ee.evaluate(vals);
        output.emit(String.valueOf(result));
      }
      catch (InvocationTargetException ex) {
        logger.error("uh oh! while process:", ex);
      }
    }

  };
  private static final Logger logger = LoggerFactory.getLogger(RulesOperator.class);
}

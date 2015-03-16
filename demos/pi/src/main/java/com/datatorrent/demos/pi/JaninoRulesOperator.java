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
public class JaninoRulesOperator extends BaseOperator
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  private transient ExpressionEvaluator ee;
  private String expression = "a > b ? a : b";

  @Override
  public void setup(OperatorContext context)
  {
    createExpression();
  }

  private void createExpression() {
    try {
      ee = new ExpressionEvaluator(
              expression, // expression
              int.class, // expressionType
              new String[] {"a", "b", "c", "d", "e"}, // parameterNames
              new Class[] {int.class, int.class, int.class, int.class, int.class} // parameterTypes
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
        Object[] vals = new Object[5];
        vals[0] = tuple;
        vals[1] = 30;
        vals[2] = 40;
        vals[3] = 50;
        vals[4] = 60;

        int result = (Integer)ee.evaluate(vals);
        output.emit(String.valueOf(result));
      }
      catch (InvocationTargetException ex) {
        logger.error("uh oh! while process:", ex);
      }
    }

  };

  public String getExpression()
  {
    return expression;
  }

  public void setExpression(String expression)
  {
    logger.info("setting expression :: ", expression);
    this.expression = expression;
  }


  private static final Logger logger = LoggerFactory.getLogger(JaninoRulesOperator.class);
}

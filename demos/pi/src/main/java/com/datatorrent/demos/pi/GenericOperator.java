/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.DTThrowable;
import com.esotericsoftware.reflectasm.MethodAccess;
import java.lang.reflect.Method;
import java.util.logging.Level;
import org.codehaus.janino.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class GenericOperator extends BaseOperator
{
  public String expression;
  public Class<?> inputClass;
  public Class<?> outputClass;
  private transient MethodAccess access;
  private transient Object methodClass;
  private int methodIdx;
  private Object outputBean;
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object input)
    {
      processTuple(input);
    }

  };
  public DefaultOutputPort<Object> output = new DefaultOutputPort<Object>();

  @Override
  public void setup(OperatorContext cntxt)
  {
    try {
      ExpressionEvaluator ee = new ExpressionEvaluator();
      ee.setStaticMethod(false);
      ee.setParameters(new String[] {"input", "output"}, new Class[] {inputClass, outputClass});
      ee.setExpressionType(void.class);
      ee.cook(expression);
      Method method = ee.getMethod();

      methodClass = method.getDeclaringClass().newInstance();
      access = MethodAccess.get(method.getDeclaringClass());
      methodIdx = access.getIndex(method.getName());

    }
    catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    try {
      outputBean = outputClass.newInstance();
    }
    catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }
  }

  private void processTuple(Object input)
  {
    try {
      access.invoke(methodClass, methodIdx, new Object[] {input, outputBean});
    }
    catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }
  }

  @Override
  public void endWindow()
  {
    output.emit(outputBean);
  }

  private static final Logger logger = LoggerFactory.getLogger(GenericOperator.class);
}

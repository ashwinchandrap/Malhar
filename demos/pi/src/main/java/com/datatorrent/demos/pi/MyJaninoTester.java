/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import com.datatorrent.demos.pi.MyJaninoTester.CustomData;
import java.lang.reflect.InvocationTargetException;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.IExpressionEvaluator;
import org.codehaus.janino.ExpressionEvaluator;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class MyJaninoTester
{
  public ExpressionEvaluator ee = null;

  public MyJaninoTester() {
    try {
      ee = new ExpressionEvaluator(
              "a.getValue() > b.getValue() ? true : false", // expression
              Boolean.class, // expressionType
              new String[] {"a", "b"}, // parameterNames
              new Class[] {com.datatorrent.demos.pi.MyJaninoTester.CustomData.class, com.datatorrent.demos.pi.MyJaninoTester.CustomData.class} // parameterTypes
              );
    }
    catch (CompileException ex) {
      throw new RuntimeException(ex);
    }

  }

  public static void main(String[] args)
  {
    try {
      MyJaninoTester tester = new MyJaninoTester();
      Object[] vals = new Object[2];
      MyInteger a = new MyInteger();
      a.val = 60;
      vals[0] = a;
      MyLong b = new MyLong();
      b.val = 50L;
      vals[1] = b;
      Object result = tester.ee.evaluate(vals);
      System.out.println("result = " + result);

      //b.getValue().doubleValue();.

    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static interface CustomData<T> {
    public Number getValue();
  }
  public static class MyInteger implements CustomData<Integer> {
    public int val = 0;

    @Override
    public String toString()
    {
      return "MyInteger{" + "val=" + val + '}';
    }

    @Override
    public Integer getValue()
    {
      return val;
    }

  }

  public static class MyLong implements CustomData<Long> {
    public long val = 0L;

    @Override
    public Long getValue()
    {
      return val;
    }

    @Override
    public String toString()
    {
      return "MyLong{" + "val=" + val + '}';
    }
  }
}

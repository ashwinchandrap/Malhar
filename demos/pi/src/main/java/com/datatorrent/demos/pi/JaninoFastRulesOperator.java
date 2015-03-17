/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class JaninoFastRulesOperator extends BaseOperator
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  private transient Foo foo;
  private transient int[] vals = new int[2];

  @Override
  public void setup(OperatorContext context)
  {
    createExpression();
  }

  private void createExpression()
  {
    try {
      IScriptEvaluator se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
      foo = (Foo)se.createFastEvaluator("return arr[0] > arr[1] ? true : false;", Foo.class, new String[] {"arr"});
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer tuple)
    {
      vals[0] = tuple;
      vals[1] = 50;
      foo.bar(vals);
    }

  };

  public interface Foo
  {
    public boolean bar(int[] arr);

  }

  private static final Logger logger = LoggerFactory.getLogger(JaninoFastRulesOperator.class);
}

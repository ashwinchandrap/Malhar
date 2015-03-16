/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.demos.pi.MyEasyRulesTester.CompareNumberRule;
import org.easyrules.core.AnnotatedRulesEngine;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class EasyRulesOperator extends BaseOperator
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  private transient CompareNumberRule rule;
  private transient AnnotatedRulesEngine engine;
  @Override
  public void setup(OperatorContext context)
  {
    rule = new CompareNumberRule();

    engine = new AnnotatedRulesEngine();
    engine.registerRule(rule);
  }

  public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer tuple) {
      rule.a = tuple;
      rule.b = 50;

      engine.fireRules();
      output.emit(String.valueOf(rule.result));
    }
  };

}

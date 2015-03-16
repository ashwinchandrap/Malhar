/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import org.easyrules.annotation.Action;
import org.easyrules.annotation.Condition;
import org.easyrules.annotation.Rule;
import org.easyrules.core.AnnotatedRulesEngine;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class MyEasyRulesTester
{
  public static void main(String[] args)
  {
    CompareNumberRule rule = new CompareNumberRule();
    rule.a = 50;
    rule.b = 60;

    AnnotatedRulesEngine engine = new AnnotatedRulesEngine();
    engine.registerRule(rule);
    engine.fireRules();

    System.out.println("result = " + rule.result);
  }

  @Rule(name = "name", description = "desc")
  public static class CompareNumberRule {
    public int a;
    public int b;

    public int result = 0;

    @Condition
    public boolean compare() {
      if(a > b) {
        result = a;
        return true;
      } else {
        result = b;
        return false;
      }
    }

    @Action
    public void emitResult() {
    }
  }
}

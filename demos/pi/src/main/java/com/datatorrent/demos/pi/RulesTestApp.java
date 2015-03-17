/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
@ApplicationAnnotation(name = "RulesTest")
public class RulesTestApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomEventGenerator generator = dag.addOperator("NumberGenerator", new RandomEventGenerator());

    JaninoRulesOperator rules = dag.addOperator("Rules", new JaninoRulesOperator());
    //EasyRulesOperator rules = dag.addOperator("Rules", new EasyRulesOperator());

    rules.setParameterNames("a:b:c:d:e:range");
    rules.setParameterTypes("int:int:int:int:int:com.datatorrent.demos.pi.JaninoRulesOperator$Range");
    rules.setReturnType("boolean");
    rules.setExpression("a > b ? a : b");
    /*
     int.class, // expressionType
     new String[] {"a", "b", "c", "d", "e"}, // parameterNames
     new Class[] {int.class, int.class, int.class, int.class, int.class} // parameterTypes
     "a > b ? a : b" // expression
     */
    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("applyRules", generator.integer_data, rules.input);
    dag.addStream("printResults", rules.output, console.input);
  }

}

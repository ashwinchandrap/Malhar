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
import com.datatorrent.demos.pi.JaninoRulesOperator.Range;
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

    //EasyRulesOperator rules = dag.addOperator("Rules", new EasyRulesOperator());
    JaninoFastRulesOperator rules = dag.addOperator("Rules", new JaninoFastRulesOperator());
    /*
    JaninoRulesOperator rules = dag.addOperator("Rules", new JaninoRulesOperator());
    Range range = new Range();
    range.setRange("1:2:3:4:5");
    rules.setRange(range);
    rules.setParameterNames("a:b:c:d:e:range");
    rules.setParameterTypes("int:int:int:int:int:com.datatorrent.demos.pi.JaninoRulesOperator$Range");
    rules.setReturnType("boolean");
    rules.setExpression("a > b ? true : false");
    */

    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("applyRules", generator.integer_data, rules.input);
    dag.addStream("printResults", rules.output, console.input);
  }

}

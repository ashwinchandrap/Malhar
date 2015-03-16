/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class RulesTestApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomEventGenerator generator = dag.addOperator("NumberGenerator", new RandomEventGenerator());

    //JaninoRulesOperator rules = dag.addOperator("Rules", new JaninoRulesOperator());
    EasyRulesOperator rules = dag.addOperator("Rules", new EasyRulesOperator());

    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("applyRules", generator.integer_data, rules.input);
    dag.addStream("printResults", rules.output, console.input);
  }

}

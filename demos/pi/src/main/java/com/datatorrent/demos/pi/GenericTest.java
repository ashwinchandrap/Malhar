/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.parser.AbstractCsvParser.Field;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class GenericTest implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration c)
  {
    RandomEmployeeGenerator generator = dag.addOperator("generator", new RandomEmployeeGenerator());

    CsvToPojoOperator parser = dag.addOperator("parser", new CsvToPojoOperator());
    parser.beanClass = Employee.class;

    ArrayList<Field> list = new ArrayList<Field>();

    Field field = new Field();
    field.setName("id");
    field.setType("LONG");
    list.add(field);

    field = new Field();
    field.setName("name");
    field.setType("STRING");
    list.add(field);

    field = new Field();
    field.setName("salary");
    field.setType("DOUBLE");
    list.add(field);
    parser.setFields(list);

    GenericOperator computer = dag.addOperator("genericOper", new GenericOperator());
    computer.inputClass = Employee.class;
    computer.outputClass = Employee.class;
    computer.expression = "output.salary += input.salary";

    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());

    dag.addStream("parse", generator.output, parser.input);
    dag.addStream("compute", parser.output, computer.input);
    dag.addStream("print", computer.output, console.input);

  }

}

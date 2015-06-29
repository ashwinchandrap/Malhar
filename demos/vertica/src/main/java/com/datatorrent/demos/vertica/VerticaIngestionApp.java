/**
 * Put your copyright and license info here.
 */
package com.datatorrent.demos.vertica;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.contrib.vertica.FileMeta;
import com.datatorrent.contrib.vertica.JdbcBatchInsertOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "VerticaIngestionApp")
public class VerticaIngestionApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    TableDataGenerator rowGenerator = dag.addOperator("Generator", new TableDataGenerator());
    //ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    HdfsWriter hdfsWriter = dag.addOperator("HdfsWriter", new HdfsWriter());
    JdbcBatchInsertOperator<FileMeta> verticaWriter = dag.addOperator("VerticaWriter", new JdbcBatchInsertOperator<FileMeta>());
    verticaWriter.setParser(new TableMappingParser());

    //dag.addStream("printtables", rowGenerator.randomBatchOutput, console.input);
    dag.addStream("generatedData", rowGenerator.randomBatchOutput, hdfsWriter.input);
    dag.addStream("toVertica", hdfsWriter.offsetOutput, verticaWriter.input);
  }
}

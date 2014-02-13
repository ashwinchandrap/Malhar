/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.lib.algo.TopN;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.logs.DimensionObject;
import com.datatorrent.lib.stream.JsonByteArrayOperator;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class Application1 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration c)
  {
    LogstreamPropertyRegistry registry = new LogstreamPropertyRegistry();
    // set app name
    dag.setAttribute(DAG.APPLICATION_NAME, "Logstream Application");
    //dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);

    RabbitMQLogsInputOperator logInput = dag.addOperator("LogInput", new RabbitMQLogsInputOperator());
    logInput.setRegistry(registry);
    logInput.addPropertiesFromString(new String[] {"localhost", "logsExchange", "direct", "logs", "apache:mysql:syslog:system"});
    //logInput.addPropertiesFromString(new String[] {"localhost", "logsExchange", "direct", "logs", "apache:system"});

    JsonByteArrayOperator jsonToMap = dag.addOperator("JsonToMap", new JsonByteArrayOperator());
    jsonToMap.setConcatenationCharacter('_');

    FilterOperator filterOperator = dag.addOperator("FilterOperator", new FilterOperator());
    filterOperator.setRegistry(registry);
    filterOperator.addFilterCondition(new String[] {"type=apache", "response", "response.equals(\"404\")"});
    filterOperator.addFilterCondition(new String[] {"type=apache", "agentinfo_name", "agentinfo_name.equals(\"Firefox\")"});
    filterOperator.addFilterCondition(new String[] {"type=apache", "default=true"});
    filterOperator.addFilterCondition(new String[] {"type=mysql", "default=true"});
    filterOperator.addFilterCondition(new String[] {"type=syslog", "default=true"});
    filterOperator.addFilterCondition(new String[] {"type=system", "default=true"});

    DimensionOperator dimensionOperator = dag.addOperator("DimensionOperator", new DimensionOperator());
    dimensionOperator.setRegistry(registry);
    String[] dimensionInputString1 = new String[] {"type=apache", "timebucket=m", "request", "clientip", "clientip:request", "values=bytes.sum:bytes.avg"};
    String[] dimensionInputString2 = new String[] {"type=system", "timebucket=m", "disk", "values=writes.avg"};
    String[] dimensionInputString3 = new String[] {"type=syslog", "timebucket=m", "program", "values=pid.count"};
    dimensionOperator.setDimensionsFromString(dimensionInputString1);
    dimensionOperator.setDimensionsFromString(dimensionInputString2);
    dimensionOperator.setDimensionsFromString(dimensionInputString3);

    LogstreamTopN topN = dag.addOperator("TopN", new LogstreamTopN());
    topN.setN(10);
    topN.setRegistry(registry);

    ConsoleOutputOperator consoleOut = dag.addOperator("ConsoleOut", new ConsoleOutputOperator());
    //consoleOut.silent = true;

    dag.addStream("inputJSonToMap", logInput.outputPort, jsonToMap.input);
    //dag.addStream("inputJSonToMap", logInput.outputPort, consoleOut.input);
    //dag.addStream("consoleout", jsonToMap.outputFlatMap, consoleOut.input);
    dag.addStream("toFilterOper", jsonToMap.outputFlatMap, filterOperator.input);
    //dag.addStream("consoleout", filterOperator.outputMap, consoleOut.input);
    dag.addStream("toDimensionOper", filterOperator.outputMap, dimensionOperator.in);
    //dag.addStream("consoleout", dimensionOperator.aggregationsOutput, consoleOut.input);
    dag.addStream("toTopN", dimensionOperator.aggregationsOutput, topN.data);
    dag.addStream("consoleout", topN.top, consoleOut.input);

    dag.setInputPortAttribute(jsonToMap.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterOperator.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(consoleOut.input, PortContext.PARTITION_PARALLEL, true);
    //dag.addStream("toConsole", topN.top, consoleOut.input);

  }

}

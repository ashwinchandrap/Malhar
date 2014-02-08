/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
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

    JsonByteArrayOperator jsonToMap = dag.addOperator("JsonToMap", new JsonByteArrayOperator());
    jsonToMap.setConcatenationCharacter('_');

    FilterOperator filterOperator = dag.addOperator("FilterOperator", new FilterOperator());
    filterOperator.setRegistry(registry);
    filterOperator.addFilterCondition(new String[]{"type=apache", "response", "response.equals(\"404\")"});
    filterOperator.addFilterCondition(new String[]{"type=apache", "agentinfo_name", "agentinfo_name.equals(\"Firefox\")"});


    /*
     DimensionOperator dimensionOperator = dag.addOperator("DimensionOperator", new DimensionOperator());
     dimensionOperator.setRegistry(registry);
     String[] dimensionInputString = new String[]{"type=apache","timebucket=s","timebucket=h","request,clientip,clientip:request","values=bytes.sum:bytes.avg"};
     dimensionOperator.setDimensionsFromString(dimensionInputString);
     */

    /*
     TopN<String, DimensionObject<String>> topN = dag.addOperator("TopN", new TopN<String, DimensionObject<String>>());
     */
    ConsoleOutputOperator consoleOut = dag.addOperator("ConsoleOut", new ConsoleOutputOperator());
    //consoleOut.silent = true;

    dag.addStream("inputJSonToMap", logInput.outputPort, jsonToMap.input);
    //dag.addStream("inputJSonToMap", logInput.outputPort, consoleOut.input);
    //dag.addStream("consoleout", jsonToMap.outputFlatMap, consoleOut.input);
    dag.addStream("toFilterOper", jsonToMap.outputFlatMap, filterOperator.input);
    dag.addStream("consoleout", filterOperator.outputMap, consoleOut.input);
    //dag.addStream("toDimensionOper", filterOperator.outputMap, dimensionOperator.in);
    //dag.addStream("toTopN", dimensionOperator.aggregationsOutput, topN.data);

    dag.setInputPortAttribute(jsonToMap.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterOperator.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(consoleOut.input, PortContext.PARTITION_PARALLEL, true);
    //dag.addStream("toConsole", topN.top, consoleOut.input);

  }

}

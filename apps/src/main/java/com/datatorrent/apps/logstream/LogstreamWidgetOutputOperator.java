/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.apps.logstream.PropertyRegistry.PropertyRegistry;
import com.datatorrent.lib.io.WidgetOutputOperator;
import com.datatorrent.lib.logs.DimensionObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.tuple.MutablePair;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class LogstreamWidgetOutputOperator extends WidgetOutputOperator
{
  @NotNull
  private PropertyRegistry<String> registry;

  @InputPortFieldAnnotation(name = "logstream topN input", optional = true)
  public final transient LogstreamTopNInputPort logstreamTopNInput = new LogstreamTopNInputPort(LogstreamWidgetOutputOperator.this);

  public void setRegistry(PropertyRegistry<String> registry)
  {
    this.registry = registry;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    LogstreamPropertyRegistry.setInstance(registry);
  }

  public class LogstreamTopNInputPort extends DefaultInputPort<HashMap<String, ArrayList<DimensionObject<String>>>>
  {
    private LogstreamWidgetOutputOperator operator;

    public LogstreamTopNInputPort(LogstreamWidgetOutputOperator oper)
    {
      operator = oper;
    }

    @Override
    public void process(HashMap<String, ArrayList<DimensionObject<String>>> tuple)
    {
      for (Entry<String, ArrayList<DimensionObject<String>>> entry : tuple.entrySet()) {
        String keyString = entry.getKey();

        ArrayList<DimensionObject<String>> arrayList = entry.getValue();

        HashMap<String, Object> schemaObj = new HashMap<String, Object>();

        String[] keyInfo = keyString.split("\\|");
        HashMap<String, String> appMeta = new HashMap<String, String>();
        appMeta.put("timeBucket", keyInfo[0]);
        //appMeta.put("timeStamp", key[1]);
        appMeta.put("logType", registry.lookupValue(new Integer(keyInfo[2])));
        appMeta.put("filter", registry.lookupValue(new Integer(keyInfo[3])));
        appMeta.put("dimension", registry.lookupValue(new Integer(keyInfo[4])));
        String[] val = keyInfo[5].split("\\.");
        appMeta.put("value", val[0]);
        appMeta.put("operation", val[1]);

        schemaObj.put("TupleMeta", appMeta);

        String topic = keyInfo[0] + "|" + keyInfo[2] + "|" + keyInfo[3] + "|" + keyInfo[4] + "|" + keyInfo[5];

        LogstreamTopNInputPort.this.setTopic(topic);

        HashMap<String, Number> topNMap = new HashMap<String, Number>();

        for (DimensionObject<String> dimensionObject : arrayList) {
          topNMap.put(dimensionObject.getVal(), dimensionObject.getCount());
        }

        this.processTopN(topNMap, schemaObj);

      }

    }

    private void processTopN(HashMap<String, Number> topNMap, HashMap<String, Object> schemaObj)
    {
      @SuppressWarnings("unchecked")
      HashMap<String, Object>[] result = new HashMap[topNMap.size()];
      int j = 0;
      for (Entry<String, Number> e : topNMap.entrySet()) {
        result[j] = new HashMap<String, Object>();
        result[j].put("name", e.getKey());
        result[j++].put("value", e.getValue());
      }
      if (operator.isWebSocketConnected) {
        schemaObj.put("type", "topN");
        schemaObj.put("n", operator.nInTopN);
        operator.wsoo.input.process(new MutablePair<String, Object>(operator.getFullTopic(operator.topNTopic, schemaObj), result));
      }
      else {
        operator.coo.input.process(topNMap);
      }

    }

    public LogstreamTopNInputPort setN(int n)
    {
      operator.nInTopN = n;
      return this;
    }

    public LogstreamTopNInputPort setTopic(String topic)
    {
      operator.topNTopic = topic;
      return this;
    }

  }


}

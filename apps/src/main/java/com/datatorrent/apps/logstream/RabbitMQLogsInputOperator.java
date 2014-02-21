/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.apps.logstream;

import java.util.*;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitionable;

import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.contrib.rabbitmq.AbstractSinglePortRabbitMQInputOperator;

/**
 *
 * Input operator to consume logs messages from RabbitMQ
 * This operator is partitionable, each partition will receive messages from the routing key its assigned.
 */
public class RabbitMQLogsInputOperator extends AbstractSinglePortRabbitMQInputOperator<byte[]> implements Partitionable<RabbitMQLogsInputOperator>
{
  private static final Logger logger = LoggerFactory.getLogger(RabbitMQLogsInputOperator.class);
  private String[] routingKeys;
  private LogstreamPropertyRegistry registry;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    LogstreamPropertyRegistry.setInstance(registry);
  }

  @Override
  public byte[] getTuple(byte[] message)
  {
    String inputString = new String(message);
    try {
      JSONObject jSONObject = new JSONObject(inputString);
      int typeId = registry.getIndex(LogstreamUtil.LOG_TYPE, RabbitMQLogsInputOperator.this.routingKey);
      jSONObject.put(LogstreamUtil.LOG_TYPE, typeId);
      String outputString = jSONObject.toString();
      message = outputString.getBytes();
    }
    catch (Throwable ex) {
      if (ex instanceof Error) {
        throw (Error)ex;
      }
      if (ex instanceof RuntimeException) {
        throw (RuntimeException)ex;
      }
      throw new RuntimeException(ex);
    }

    return message;
  }

  /**
   * Supply the properties to the operator.
   * The properties include hostname, exchange, exchangeType, queueName and colon separated routing keys specified in the following format
   * hostName, exchange, exchangeType, queueName, routingKey1[:routingKey2]
   * @param props
   */
  public void addPropertiesFromString(String[] props)
  {
    //input string format
    //host, exchange, exchangeType, queueName, routingKey1:routingKey2:routingKey3
    //eg: localhost, logstash, direct, logs, apache:mysql:syslog

    host = props[0];
    exchange = props[1];
    exchangeType = props[2];
    queueName = props[3];

    if (props[4] != null) {
      RabbitMQLogsInputOperator.this.routingKeys = props[4].split(":");
      for (String rKey : routingKeys) {
        registry.bind(LogstreamUtil.LOG_TYPE, rKey);
      }
    }
  }

  /**
   * supply the registry object which is used to store and retrieve meta information about each tuple
   * @param registry
   */
  public void setRegistry(LogstreamPropertyRegistry registry)
  {
    this.registry = registry;
  }

  @Override
  protected RabbitMQLogsInputOperator clone() throws CloneNotSupportedException
  {
    RabbitMQLogsInputOperator oper = new RabbitMQLogsInputOperator();
    oper.host = RabbitMQLogsInputOperator.this.host;
    oper.exchange = RabbitMQLogsInputOperator.this.exchange;
    oper.exchangeType = RabbitMQLogsInputOperator.this.exchangeType;
    oper.registry = RabbitMQLogsInputOperator.this.registry;
    oper.routingKeys = RabbitMQLogsInputOperator.this.routingKeys;
    oper.routingKey = RabbitMQLogsInputOperator.this.routingKey;
    oper.queueName = RabbitMQLogsInputOperator.this.queueName;

    return oper;
  }

  /**
   * Partitions count will be the number of input routing keys.
   * Each partition receives tuples from its routing key.
   * @param clctn
   * @param i
   * @return
   */
  @Override
  public Collection<Partition<RabbitMQLogsInputOperator>> definePartitions(Collection<Partition<RabbitMQLogsInputOperator>> clctn, int i)
  {
    ArrayList<Partition<RabbitMQLogsInputOperator>> newPartitions = new ArrayList<Partition<RabbitMQLogsInputOperator>>();
    for (String rKey : routingKeys) {
      try {
        RabbitMQLogsInputOperator oper = RabbitMQLogsInputOperator.this.clone();
        oper.routingKey = rKey;
        oper.queueName = rKey;

        Partition<RabbitMQLogsInputOperator> partition = new DefaultPartition<RabbitMQLogsInputOperator>(oper);
        newPartitions.add(partition);
      }
      catch (Throwable ex) {
        DTThrowable.rethrow(ex);
      }
    }
    return newPartitions;
  }

}

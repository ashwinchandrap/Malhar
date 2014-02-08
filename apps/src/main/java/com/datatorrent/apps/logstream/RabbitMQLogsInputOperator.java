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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitionable;
import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.contrib.rabbitmq.AbstractSinglePortRabbitMQInputOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 *
 * Input operator to consume logs messages from RabbitMQ
 */
public class RabbitMQLogsInputOperator extends AbstractSinglePortRabbitMQInputOperator<byte[]> implements Partitionable<RabbitMQLogsInputOperator>
{
  private static final Logger logger = LoggerFactory.getLogger(RabbitMQLogsInputOperator.class);
  private String[] routingKeys;
  private LogstreamPropertyRegistry registry;

  @Override
  public byte[] getTuple(byte[] message)
  {
    String inputString = new String(message);
    try {
      JSONObject jSONObject = new JSONObject(inputString);
      int typeId = registry.getIndex("LOG_TYPE", RabbitMQLogsInputOperator.this.routingKey);
      jSONObject.put("LOG_TYPE", typeId);
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

  /*
   @Override
   public void activate(OperatorContext ctx)
   {
   if (routingKeys == null) {
   super.activate(ctx);
   }
   else {
   try {
   connFactory = new ConnectionFactory();
   connFactory.setHost(host);
   connection = connFactory.newConnection();
   channel = connection.createChannel();

   channel.exchangeDeclare(exchange, exchangeType);
   if (queueName == null) {
   // unique queuename is generated
   // used in case of fanout exchange
   queueName = channel.queueDeclare().getQueue();
   }
   else {
   // user supplied name
   // used in case of direct exchange
   channel.queueDeclare(queueName, true, false, false, null);
   }

   for (String rKey : routingKeys) {
   channel.queueBind(queueName, exchange, rKey);
   }

   //      consumer = new QueueingConsumer(channel);
   //      channel.basicConsume(queueName, true, consumer);
   tracingConsumer = new TracingConsumer(channel);
   cTag = channel.basicConsume(queueName, true, tracingConsumer);
   }
   catch (IOException ex) {
   logger.debug(ex.toString());
   }
   }
   }
   */
  /*
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    logstreamHoldingBuffer = new HashMap<String, ArrayBlockingQueue<byte[]>>();
    for (String rKey : routingKeys) {
      ArrayBlockingQueue<byte[]> buffer = new ArrayBlockingQueue<byte[]>(bufferSize);
      logstreamHoldingBuffer.put(rKey, buffer);
    }
    logger.info("exchange = {}", this.getExchange());
    logger.info("host name = {}", this.getHost());
    logger.info("exchange type = {}", this.getExchangeType());
  }
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
        registry.bind("LOG_TYPE", rKey);
      }
    }
    System.out.println("added properties...host = " + host + " exchange = " + exchange + " exchangeType = " + exchangeType + " queue name = " + queueName);
  }

  public void setRegistry(LogstreamPropertyRegistry registry)
  {
    this.registry = registry;
  }

  @Override
  public Collection<Partition<RabbitMQLogsInputOperator>> definePartitions(Collection<Partition<RabbitMQLogsInputOperator>> clctn, int i)
  {
    ArrayList<Partition<RabbitMQLogsInputOperator>> newPartitions = new ArrayList<Partition<RabbitMQLogsInputOperator>>();
    for (String rKey : routingKeys) {
      RabbitMQLogsInputOperator oper = new RabbitMQLogsInputOperator();
      oper.routingKey = rKey;
      oper.host = host;
      oper.exchange = exchange;
      oper.exchangeType = exchangeType;
      oper.queueName = rKey;
      oper.registry = registry;
      oper.routingKeys = routingKeys;

      Partition<RabbitMQLogsInputOperator> partition = new DefaultPartition<RabbitMQLogsInputOperator>(oper);

      newPartitions.add(partition);
      System.out.println("added new partition for logs input");
    }
    return newPartitions;
  }

}

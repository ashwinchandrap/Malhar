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

import java.net.URI;
import java.util.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.algo.TopN;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;
import com.datatorrent.lib.logs.DimensionObject;
import com.datatorrent.lib.logs.MultiWindowDimensionAggregation;
import com.datatorrent.lib.logs.MultiWindowDimensionAggregation.AggregateOperation;
import com.datatorrent.lib.stream.Counter;
import com.datatorrent.lib.stream.JsonByteArrayOperator;
import com.datatorrent.lib.streamquery.SelectOperator;
import com.datatorrent.lib.streamquery.condition.EqualValueCondition;
import com.datatorrent.lib.streamquery.index.ColumnIndex;
import com.datatorrent.lib.util.AbstractDimensionTimeBucketOperator;
import com.datatorrent.lib.util.DimensionTimeBucketSumOperator;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.redis.RedisKeyValPairOutputOperator;
import com.datatorrent.contrib.redis.RedisMapOutputOperator;
import com.datatorrent.contrib.redis.RedisNumberSummationMapOutputOperator;


/**
 * Log stream processing application based on DataTorrent platform.<br>
 * This application consumes log data generated by running systems and services
 * in near real-time, and processes it to produce actionable data. This in turn
 * can be used to produce alerts, take corrective actions, or predict system
 * behavior.
 * <p>
 * Running Java Test or Main app in IDE:
 *
 * <pre>
 * LocalMode.runApp(new Application(), 600000); // 10 min run
 * </pre>
 *
 * Output : <br>
 * During successful deployment and run, user should see following output:
 * TODO
 * <pre>
 * </pre>
 *
 * Application DAG : <br>
 * TODO
 * <img src="doc-files/Application.gif" width=600px > <br>
 * <br>
 *
 * Streaming Window Size : 1000 ms(1 Sec) <br>
 * Operator Details : <br>
 * <ul>
 * <li><b>The operator Console: </b> This operator just outputs the multiWindowDimensionInput tuples
 * to the console (or stdout). You can use other output adapters if needed.<br>
 * </li>
 * </ul>
 *
 * @since 0.3.5
 */
public class Application implements StreamingApplication
{
  private InputPort<Object> wsOutput(DAG dag, String operatorName)
  {
    String daemonAddress = dag.getValue(DAG.GATEWAY_ADDRESS);
    if (!StringUtils.isEmpty(daemonAddress)) {
      URI uri = URI.create("ws://" + daemonAddress + "/pubsub");
      String appId = "appid";
      //appId = dag.attrValue(DAG.APPLICATION_ID, null); // will be used once UI is able to pick applications from list and listen to corresponding application
      String topic = "apps.logstream." + appId + "." + operatorName;
      //LOG.info("WebSocket with daemon at: {}", daemonAddress);
      PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator(operatorName, new PubSubWebSocketOutputOperator<Object>());
      wsOut.setUri(uri);
      wsOut.setTopic(topic);
      return wsOut.input;
    }
    ConsoleOutputOperator operator = dag.addOperator(operatorName, new ConsoleOutputOperator());
    operator.setStringFormat(operatorName + ": %s");
    return operator.input;
  }

  public InputPort<Map<String, Map<String, Number>>> getRedisOutput(String name, DAG dag, int dbIndex)
  {
    @SuppressWarnings("unchecked")
    RedisNumberSummationMapOutputOperator<String, Map<String, Number>> oper = dag.addOperator(name, RedisNumberSummationMapOutputOperator.class);
    oper.getStore().setDbIndex(dbIndex);
    return oper.input;
  }

  public DimensionTimeBucketSumOperator getApacheDimensionTimeBucketSumOperator(String name, DAG dag)
  {
    DimensionTimeBucketSumOperator oper = dag.addOperator(name, DimensionTimeBucketSumOperator.class);
    oper.addDimensionKeyName("host");                   // 0
    oper.addDimensionKeyName("clientip");               // 1
    oper.addDimensionKeyName("request");                // 2 url
    oper.addDimensionKeyName("response");               // 3
    oper.addDimensionKeyName("referrer");               // 4
    oper.addDimensionKeyName("geoip.country_name");     // 5
    oper.addDimensionKeyName("agentinfo.os");           // 6
    oper.addDimensionKeyName("agentinfo.name");         // 7 browser

    oper.addValueKeyName("bytes");

    // dimension set # 1
    // url
    Set<String> dimensionKey1 = new HashSet<String>();
    dimensionKey1.add("request");

    // dimension set # 2
    // ip
    Set<String> dimensionKey2 = new HashSet<String>();
    dimensionKey2.add("clientip");

    // dimension set # 3
    // url, ip
    Set<String> dimensionKey3 = new HashSet<String>();
    dimensionKey3.add("clientip");
    dimensionKey3.add("request");

    // dimension set # 4
    // country, os, browser
    Set<String> dimensionKey4 = new HashSet<String>();
    dimensionKey4.add("geoip.country_name");
    dimensionKey4.add("agentinfo.os");
    dimensionKey4.add("agentinfo.name");

    // dimension set # 5
    // country, url --> sum of bytes
    Set<String> dimensionKey5 = new HashSet<String>();
    dimensionKey5.add("request");
    dimensionKey5.add("geoip.country_name");

    // dimension set # 6
    // os, browser --> sum of bytes
    Set<String> dimensionKey6 = new HashSet<String>();
    dimensionKey6.add("agentinfo.os");
    dimensionKey6.add("agentinfo.name");

    // dimension set # 7
    // os
    Set<String> dimensionKey7 = new HashSet<String>();
    dimensionKey7.add("agentinfo.os");

    // dimension set # 8
    // browser
    Set<String> dimensionKey8 = new HashSet<String>();
    dimensionKey8.add("agentinfo.name");

    // dimension set # 9
    // host
    Set<String> dimensionKey9 = new HashSet<String>();
    dimensionKey9.add("host");

    // dimension set # 10
    // request, response
    Set<String> dimensionKey10 = new HashSet<String>();
    dimensionKey10.add("request");
    dimensionKey10.add("response");

    // dimension set # 11
    // host, response
    Set<String> dimensionKey11 = new HashSet<String>();
    dimensionKey11.add("host");
    dimensionKey11.add("response");

    // dimension set # 12
    // host, response
    Set<String> dimensionKey12 = new HashSet<String>();
    dimensionKey12.add("response");

    try {
      oper.addCombination(dimensionKey1);
      oper.addCombination(dimensionKey2);
      oper.addCombination(dimensionKey3);
      oper.addCombination(dimensionKey4);
      oper.addCombination(dimensionKey5);
      oper.addCombination(dimensionKey6);
      oper.addCombination(dimensionKey7);
      oper.addCombination(dimensionKey8);
      oper.addCombination(dimensionKey9);
      oper.addCombination(dimensionKey10);
      oper.addCombination(dimensionKey11);
      oper.addCombination(dimensionKey12);
    }
    catch (NoSuchFieldException e) {
    }

    oper.setTimeBucketFlags(AbstractDimensionTimeBucketOperator.TIMEBUCKET_MINUTE);
    return oper;
  }

  private MultiWindowDimensionAggregation getApacheAggregationCountOper(String name, DAG dag)
  {
    MultiWindowDimensionAggregation oper = dag.addOperator(name, MultiWindowDimensionAggregation.class);
    oper.setWindowSize(3);
    List<int[]> dimensionArrayList = new ArrayList<int[]>();
    int[] dimensionArray1 = {2};
    int[] dimensionArray2 = {1};
    int[] dimensionArray3 = {1, 2};
    int[] dimensionArray4 = {7, 5, 6};
    int[] dimensionArray5 = {2, 5};
    int[] dimensionArray6 = {7, 6};
    int[] dimensionArray7 = {6};
    int[] dimensionArray8 = {7};
    int[] dimensionArray9 = {0};
    int[] dimensionArray10 = {3, 2};
    int[] dimensioArray11 = {3, 0};
    int[] dimensioArray12 = {3};

    dimensionArrayList.add(dimensionArray1);
    dimensionArrayList.add(dimensionArray2);
    dimensionArrayList.add(dimensionArray3);
    dimensionArrayList.add(dimensionArray4);
    dimensionArrayList.add(dimensionArray5);
    dimensionArrayList.add(dimensionArray6);
    dimensionArrayList.add(dimensionArray7);
    dimensionArrayList.add(dimensionArray8);
    dimensionArrayList.add(dimensionArray9);
    dimensionArrayList.add(dimensionArray10);
    dimensionArrayList.add(dimensioArray11);
    dimensionArrayList.add(dimensioArray12);

    oper.setDimensionArray(dimensionArrayList);

    oper.setTimeBucket("m");
    oper.setDimensionKeyVal("0"); // aggregate on count
    oper.setWindowSize(2); // 1 sec window
    // oper.setOperationType(AggregateOperation.AVERAGE);

    return oper;
  }

  private MultiWindowDimensionAggregation getApacheAggregationSumOper(String name, DAG dag)
  {
    MultiWindowDimensionAggregation oper = dag.addOperator(name, MultiWindowDimensionAggregation.class);
    oper.setWindowSize(3);
    List<int[]> dimensionArrayList = new ArrayList<int[]>();
    int[] dimensionArray1 = {1};

    dimensionArrayList.add(dimensionArray1);

    oper.setDimensionArray(dimensionArrayList);

    oper.setTimeBucket("m");
    oper.setDimensionKeyVal("1"); // aggregate on sum
    oper.setWindowSize(2); // 1 sec window

    return oper;
  }

  private AggregationsToRedisOperator<String, DimensionObject<String>> getApacheTopNToRedisOperatorCountAggregation(String name, DAG dag)
  {
    AggregationsToRedisOperator<String, DimensionObject<String>> oper = dag.addOperator(name, new AggregationsToRedisOperator<String, DimensionObject<String>>());

    HashMap<String, Integer> dimensionToDbIndexMap = new HashMap<String, Integer>();
    dimensionToDbIndexMap.put("2", 2);
    dimensionToDbIndexMap.put("1", 3);
    dimensionToDbIndexMap.put("0", 10);

    oper.setDimensionToDbIndexMap(dimensionToDbIndexMap);
    return oper;
  }

  private AggregationsToRedisOperator<String, DimensionObject<String>> getApacheTopNToRedisOperatorSumAggregation(String name, DAG dag)
  {
    AggregationsToRedisOperator<String, DimensionObject<String>> oper = dag.addOperator(name, new AggregationsToRedisOperator<String, DimensionObject<String>>());

    HashMap<String, Integer> dimensionToDbIndexMap = new HashMap<String, Integer>();
    dimensionToDbIndexMap.put("1", 6);

    oper.setDimensionToDbIndexMap(dimensionToDbIndexMap);
    return oper;
  }

  private SelectOperator getFilteredMessagesOperator(String name, DAG dag)
  {
    SelectOperator oper = dag.addOperator(name, new SelectOperator());
    oper.addIndex(new ColumnIndex("host", null));
    oper.addIndex(new ColumnIndex("request", null));
    oper.addIndex(new ColumnIndex("response", null));
    EqualValueCondition condition = new EqualValueCondition();
    condition.addEqualValue("response", "404");
    oper.setCondition(condition);

    return oper;
  }

  public DimensionTimeBucketSumOperator getFilteredApacheDimensionTimeBucketSumOperator(String name, DAG dag)
  {
    DimensionTimeBucketSumOperator oper = dag.addOperator(name, DimensionTimeBucketSumOperator.class);
    oper.addDimensionKeyName("host");                  // 0
    oper.addDimensionKeyName("request");               // 1

    // dimension set # 1
    // url
    Set<String> dimensionKey1 = new HashSet<String>();
    dimensionKey1.add("host");

    // dimension set # 2
    // ip
    Set<String> dimensionKey2 = new HashSet<String>();
    dimensionKey2.add("request");

    try {
      oper.addCombination(dimensionKey1);
      oper.addCombination(dimensionKey2);
    }
    catch (NoSuchFieldException e) {
    }

    oper.setTimeBucketFlags(AbstractDimensionTimeBucketOperator.TIMEBUCKET_MINUTE);
    return oper;
  }

  private MultiWindowDimensionAggregation getFilteredApacheAggregationCountOper(String name, DAG dag)
  {
    MultiWindowDimensionAggregation oper = dag.addOperator(name, MultiWindowDimensionAggregation.class);
    oper.setWindowSize(3);
    List<int[]> dimensionArrayList = new ArrayList<int[]>();
    int[] dimensionArray1 = {0};
    int[] dimensionArray2 = {1};

    dimensionArrayList.add(dimensionArray1);
    dimensionArrayList.add(dimensionArray2);

    oper.setDimensionArray(dimensionArrayList);

    oper.setTimeBucket("m");
    oper.setDimensionKeyVal("0"); // aggregate on count
    oper.setWindowSize(2); // 1 sec window

    return oper;
  }

  private AggregationsToRedisOperator<String, DimensionObject<String>> getFilteredApacheTopNToRedisOperatorCountAggregation(String name, DAG dag)
  {
    AggregationsToRedisOperator<String, DimensionObject<String>> oper = dag.addOperator(name, new AggregationsToRedisOperator<String, DimensionObject<String>>());

    HashMap<String, Integer> dimensionToDbIndexMap = new HashMap<String, Integer>();
    dimensionToDbIndexMap.put("0", 8);
    dimensionToDbIndexMap.put("1", 7);

    oper.setDimensionToDbIndexMap(dimensionToDbIndexMap);
    return oper;
  }

  public DimensionTimeBucketSumOperator getMysqlDimensionTimeBucketSumOperator(String name, DAG dag)
  {

    DimensionTimeBucketSumOperator oper = dag.addOperator(name, DimensionTimeBucketSumOperator.class);
    oper.addDimensionKeyName("user");
    oper.addDimensionKeyName("query_time");
    oper.addDimensionKeyName("rows_sent");
    oper.addDimensionKeyName("rows_examined");

    oper.addValueKeyName("lock_time");

    Set<String> dimensionKey = new HashSet<String>();

    dimensionKey.add("user");
    //dimensionKey.add("rows_examined");
    try {
      oper.addCombination(dimensionKey);
    }
    catch (NoSuchFieldException e) {
    }

    oper.setTimeBucketFlags(AbstractDimensionTimeBucketOperator.TIMEBUCKET_MINUTE);
    return oper;
  }

  private MultiWindowDimensionAggregation getMysqlAggregationOper(String name, DAG dag)
  {
    MultiWindowDimensionAggregation oper = dag.addOperator(name, MultiWindowDimensionAggregation.class);
    oper.setWindowSize(3);
    List<int[]> dimensionArrayList = new ArrayList<int[]>();
    int[] dimensionArray = {0};
    dimensionArrayList.add(dimensionArray);
    oper.setDimensionArray(dimensionArrayList);

    oper.setTimeBucket("m");
    oper.setDimensionKeyVal("1");

    // oper.setOperationType(AggregateOperation.AVERAGE);

    return oper;
  }

  public DimensionTimeBucketSumOperator getSyslogDimensionTimeBucketSumOperator(String name, DAG dag)
  {

    DimensionTimeBucketSumOperator oper = dag.addOperator(name, DimensionTimeBucketSumOperator.class);
    oper.addDimensionKeyName("program");
    oper.addDimensionKeyName("pid");

    oper.addValueKeyName("@version");

    Set<String> dimensionKey = new HashSet<String>();

    dimensionKey.add("program");
    //dimensionKey.add("rows_examined");
    try {
      oper.addCombination(dimensionKey);
    }
    catch (NoSuchFieldException e) {
    }

    oper.setTimeBucketFlags(AbstractDimensionTimeBucketOperator.TIMEBUCKET_MINUTE);
    return oper;
  }

  private MultiWindowDimensionAggregation getSyslogAggregationOper(String name, DAG dag)
  {
    MultiWindowDimensionAggregation oper = dag.addOperator(name, MultiWindowDimensionAggregation.class);
    oper.setWindowSize(3);
    List<int[]> dimensionArrayList = new ArrayList<int[]>();
    int[] dimensionArray = {0};
    dimensionArrayList.add(dimensionArray);
    oper.setDimensionArray(dimensionArrayList);

    oper.setTimeBucket("m");
    oper.setDimensionKeyVal("1");

    // oper.setOperationType(AggregateOperation.AVERAGE);

    return oper;
  }

  public DimensionTimeBucketSumOperator getSystemDimensionTimeBucketSumOperator(String name, DAG dag)
  {

    DimensionTimeBucketSumOperator oper = dag.addOperator(name, DimensionTimeBucketSumOperator.class);
    oper.addDimensionKeyName("host");

    oper.addValueKeyName("MemFree");
    oper.addValueKeyName("SwapFree");
    oper.addValueKeyName("system_hz");
    oper.addValueKeyName("user_hz");
    oper.addValueKeyName("io");
    oper.addValueKeyName("io_ms");
    oper.addValueKeyName("io_wait_ms");

    Set<String> dimensionKey = new HashSet<String>();

    dimensionKey.add("host");
    try {
      oper.addCombination(dimensionKey);
    }
    catch (NoSuchFieldException e) {
    }

    oper.setTimeBucketFlags(AbstractDimensionTimeBucketOperator.TIMEBUCKET_MINUTE);
    return oper;
  }

  private MultiWindowDimensionAggregation getSystemAggregationOper(String name, DAG dag)
  {
    MultiWindowDimensionAggregation oper = dag.addOperator(name, MultiWindowDimensionAggregation.class);
    oper.setWindowSize(3);
    List<int[]> dimensionArrayList = new ArrayList<int[]>();
    int[] dimensionArray = {0};
    dimensionArrayList.add(dimensionArray);
    oper.setDimensionArray(dimensionArrayList);

    oper.setTimeBucket("m");
    oper.setDimensionKeyVal("1");

    oper.setOperationType(AggregateOperation.AVERAGE);
    oper.setWindowSize(120); // 1 min window

    return oper;
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // set app name
    dag.setAttribute(DAG.APPLICATION_NAME, "Logstream Application");
    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);

    ConsoleOutputOperator mysqlConsole = dag.addOperator("MysqlConsole", ConsoleOutputOperator.class);
    ConsoleOutputOperator syslogConsole = dag.addOperator("SyslogConsole", ConsoleOutputOperator.class);
    ConsoleOutputOperator systemConsole = dag.addOperator("SystemlogConsole", ConsoleOutputOperator.class);

    LogScoreOperator logScoreOperator = dag.addOperator("logscore", new LogScoreOperator());

    /*
     * Read log file messages from a messaging system (Redis, RabbitMQ, etc)
     * Typically one message equates to a single line in a log file, but in
     * some cases may be multiple lines such as java stack trace, etc.
     */

    // Get logs from RabbitMQ
    RabbitMQLogsInputOperator apacheLogInput = dag.addOperator("ApacheLogInput", RabbitMQLogsInputOperator.class);
    RabbitMQLogsInputOperator mysqlLogInput = dag.addOperator("MysqlLogInput", RabbitMQLogsInputOperator.class);
    RabbitMQLogsInputOperator syslogLogInput = dag.addOperator("SyslogLogInput", RabbitMQLogsInputOperator.class);
    RabbitMQLogsInputOperator systemLogInput = dag.addOperator("SystemLogInput", RabbitMQLogsInputOperator.class);

    // Get logs from Redis
    //RedisBLPOPStringInputOperator redisInput = dag.addOperator("redisInput", new RedisBLPOPStringInputOperator());
    //redisInput.setHost("localhost");
    //redisInput.setPort(6379);
    //redisInput.setRedisKeys("logstash");

    // dynamically partition based on number of incoming tuples from the queue
    //dag.setAttribute(apacheLogInput, OperatorContext.INITIAL_PARTITION_COUNT, 2);
    //dag.setAttribute(apacheLogInput, OperatorContext.PARTITION_TPS_MIN, 1000);
    //dag.setAttribute(apacheLogInput, OperatorContext.PARTITION_TPS_MAX, 3000);

    /*
     * Convert incoming JSON structures to flattened map objects
     */
    JsonByteArrayOperator apacheLogJsonToMap = dag.addOperator("ApacheLogJsonToMap", JsonByteArrayOperator.class);
    dag.addStream("apache_convert_type", apacheLogInput.outputPort, apacheLogJsonToMap.input);

    JsonByteArrayOperator mysqlLogJsonToMap = dag.addOperator("MysqlLogJsonToMap", JsonByteArrayOperator.class);
    dag.addStream("mysql_convert_type", mysqlLogInput.outputPort, mysqlLogJsonToMap.input);

    JsonByteArrayOperator syslogLogJsonToMap = dag.addOperator("SyslogLogJsonToMap", JsonByteArrayOperator.class);
    dag.addStream("syslog_convert_type", syslogLogInput.outputPort, syslogLogJsonToMap.input);

    JsonByteArrayOperator systemLogJsonToMap = dag.addOperator("SystemLogJsonToMap", JsonByteArrayOperator.class);
    dag.addStream("system_convert_type", systemLogInput.outputPort, systemLogJsonToMap.input);

    // operator for tuple counter
    Counter apacheLogCounter = dag.addOperator("ApacheLogCounter", new Counter());

    /*
     * operators for sum of bytes
     */
    SumItemFromMapOperator<String, Object> apacheLogBytesSum = dag.addOperator("ApacheLogBytesSum", new SumItemFromMapOperator<String, Object>());
    apacheLogBytesSum.setSumDimension("bytes");

    /*
     * opeartor filter 404 logs
     */
    SelectOperator filter404 = getFilteredMessagesOperator("Filter404", dag);

    /*
     * Explode dimensions based on log types ( apache, mysql, syslog, etc)
     */
    DimensionTimeBucketSumOperator apacheDimensionOperator = getApacheDimensionTimeBucketSumOperator("ApacheLogDimension", dag);
    DimensionTimeBucketSumOperator apacheFilteredDimensionOperator = getFilteredApacheDimensionTimeBucketSumOperator("ApacheFilteredLogDimension", dag);
    dag.addStream("apache_dimension_in", apacheLogJsonToMap.outputFlatMap, apacheDimensionOperator.in, apacheLogCounter.input, apacheLogBytesSum.mapInput, filter404.inport);
    dag.addStream("apache_filtered_dimension_in", filter404.outport, apacheFilteredDimensionOperator.in);

    DimensionTimeBucketSumOperator mysqlDimensionOperator = getMysqlDimensionTimeBucketSumOperator("MysqlLogDimension", dag);
    dag.addStream("mysql_dimension_in", mysqlLogJsonToMap.outputFlatMap, mysqlDimensionOperator.in);

    DimensionTimeBucketSumOperator syslogDimensionOperator = getSyslogDimensionTimeBucketSumOperator("syslogLogDimension", dag);
    dag.addStream("syslog_dimension_in", syslogLogJsonToMap.outputFlatMap, syslogDimensionOperator.in);

    //DimensionTimeBucketSumOperator systemDimensionOperator = getSystemDimensionTimeBucketSumOperator("systemLogDimension", dag);
    //dag.addStream("system_dimension_in", systemLogJsonToMap.outputMap, systemDimensionOperator.in);
    /*
     * Calculate average, min, max, etc from dimensions ( based on log types )
     */
    // aggregating over sliding window
    MultiWindowDimensionAggregation apacheMultiWindowAggCountOpr = getApacheAggregationCountOper("apache_sliding_window_count", dag);
    MultiWindowDimensionAggregation apacheMultiWindowAggSumOpr = getApacheAggregationSumOper("apache_sliding_window_sum", dag);
    dag.addStream("apache_dimension_out", apacheDimensionOperator.out, apacheMultiWindowAggCountOpr.data, apacheMultiWindowAggSumOpr.data);

    MultiWindowDimensionAggregation apacheFilteredMultiWindowAggCountOpr = getFilteredApacheAggregationCountOper("apache_filtered_sliding_window_count", dag);
    dag.addStream("apache_filtered_dimension_out", apacheFilteredDimensionOperator.out, apacheFilteredMultiWindowAggCountOpr.data);

    MultiWindowDimensionAggregation mysqlMultiWindowAggOpr = getMysqlAggregationOper("mysql_sliding_window", dag);
    dag.addStream("mysql_dimension_out", mysqlDimensionOperator.out, mysqlMultiWindowAggOpr.data);

    MultiWindowDimensionAggregation syslogMultiWindowAggOpr = getSyslogAggregationOper("syslog_sliding_window", dag);
    dag.addStream("syslog_dimension_out", syslogDimensionOperator.out, syslogMultiWindowAggOpr.data);

    //MultiWindowDimensionAggregation systemMultiWindowAggOpr = getSystemAggregationOper("system_sliding_window", dag);
    //dag.addStream("system_dimension_out", systemDimensionOperator.out, systemMultiWindowAggOpr.data);

    // adding top N operator
    TopN<String, DimensionObject<String>> apacheTopNCountOpr = dag.addOperator("apache_topN_count", new TopN<String, DimensionObject<String>>());
    apacheTopNCountOpr.setN(10);
    TopN<String, DimensionObject<String>> apacheFilteredTopNCountOpr = dag.addOperator("apache_filtered_topN_count", new TopN<String, DimensionObject<String>>());
    apacheFilteredTopNCountOpr.setN(10);
    TopN<String, DimensionObject<String>> apacheTopNSumOpr = dag.addOperator("apache_topN_sum", new TopN<String, DimensionObject<String>>());
    apacheTopNSumOpr.setN(10);

    TopN<String, DimensionObject<String>> mysqlTopNOpr = dag.addOperator("mysql_topN", new TopN<String, DimensionObject<String>>());
    mysqlTopNOpr.setN(5);
    TopN<String, DimensionObject<String>> syslogTopNOpr = dag.addOperator("syslog_topN", new TopN<String, DimensionObject<String>>());
    syslogTopNOpr.setN(5);
    //TopN<String, DimensionObject<String>> systemTopNOpr = dag.addOperator("system_topN", new TopN<String, DimensionObject<String>>());
    //systemTopNOpr.setN(5);

    /*
     * Analytics Engine
     */

    dag.addStream("ApacheLogScoreCount", apacheMultiWindowAggCountOpr.output, apacheTopNCountOpr.data, logScoreOperator.apacheLogs);
    dag.addStream("ApacheFilteredLogScoreCount", apacheFilteredMultiWindowAggCountOpr.output, apacheFilteredTopNCountOpr.data);
    dag.addStream("ApacheLogScoreSum", apacheMultiWindowAggSumOpr.output, apacheTopNSumOpr.data);
    dag.addStream("MysqlLogScore", mysqlMultiWindowAggOpr.output, mysqlTopNOpr.data, logScoreOperator.mysqlLogs);
    dag.addStream("SyslogLogScore", syslogMultiWindowAggOpr.output, syslogTopNOpr.data, logScoreOperator.syslogLogs);
    //dag.addStream("SystemLogScore", systemLogJsonToMap.outputMap, logScoreOperator.systemLogs);

    /*
     * Alerts
     */
    //TODO

    /*
     * Console output for debugging purposes
     */
    //ConsoleOutputOperator console = dag.addOperator("console", ConsoleOutputOperator.class);
    //dag.addStream("topn_output", apacheTopNOpr.top);entry.getValue()

    /*
     * write to redis for siteops
     */
    // prepare operators to convert output streams to redis output format
    AggregationsToRedisOperator<Integer, Integer> apacheLogCounterToRedis = dag.addOperator("ApacheLogCounterToRedis", new AggregationsToRedisOperator<Integer, Integer>());
    apacheLogCounterToRedis.setKeyIndex(11);
    AggregationsToRedisOperator<String, String> apacheLogBytesSumToRedis = dag.addOperator("ApacheLogBytesSumToRedis", new AggregationsToRedisOperator<String, String>());
    apacheLogBytesSumToRedis.setKeyIndex(9);
    AggregationsToRedisOperator<String, DimensionObject<String>> topNCountToRedis = getApacheTopNToRedisOperatorCountAggregation("topNCountToRedis", dag);
    AggregationsToRedisOperator<String, DimensionObject<String>> filteredTopNCountToRedis = getFilteredApacheTopNToRedisOperatorCountAggregation("filteredTopNCountToRedis", dag);
    AggregationsToRedisOperator<String, DimensionObject<String>> topNSumToRedis = getApacheTopNToRedisOperatorSumAggregation("topNSumToRedis", dag);

    // convert ouputs for redis output operator and send output to web socket output
    dag.addStream("apache_log_counter", apacheLogCounter.output, apacheLogCounterToRedis.valueInput);
    dag.addStream("apache_log_bytes_sum", apacheLogBytesSum.output, apacheLogBytesSumToRedis.valueInput);
    dag.addStream("topn_redis_count", apacheTopNCountOpr.top, topNCountToRedis.multiWindowDimensionInput, wsOutput(dag, "apacheTopAggrs"));
    dag.addStream("topn_redis_sum", apacheTopNSumOpr.top, topNSumToRedis.multiWindowDimensionInput, wsOutput(dag, "apacheTopSumAggrs"));
    dag.addStream("filtered_topn_redis_count", apacheFilteredTopNCountOpr.top, filteredTopNCountToRedis.multiWindowDimensionInput);

    // redis output operators
    RedisKeyValPairOutputOperator<String, String> redisOutTotalCount = dag.addOperator("RedisOutTotalCount", new RedisKeyValPairOutputOperator<String, String>());
    redisOutTotalCount.getStore().setDbIndex(15);
    RedisKeyValPairOutputOperator<String, String> redisOutTotalSumBytes = dag.addOperator("RedisOutTotalSumBytes", new RedisKeyValPairOutputOperator<String, String>());
    redisOutTotalSumBytes.getStore().setDbIndex(15);
    RedisMapOutputOperator<String, String> redisOutTopNCount = dag.addOperator("RedisOutTopNCount", new RedisMapOutputOperator<String, String>());
    redisOutTopNCount.getStore().setDbIndex(15);
    RedisMapOutputOperator<String, String> redisOutTopNSum = dag.addOperator("RedisOutTopNSum", new RedisMapOutputOperator<String, String>());
    redisOutTopNSum.getStore().setDbIndex(15);
    RedisMapOutputOperator<String, String> redisOutFilteredTopNSum = dag.addOperator("RedisOutFilteredTopNSum", new RedisMapOutputOperator<String, String>());
    redisOutFilteredTopNSum.getStore().setDbIndex(15);

    // redis output streams
    dag.addStream("apache_log_counter_to_redis", apacheLogCounterToRedis.keyValPairOutput, redisOutTotalCount.input);
    dag.addStream("apache_log_bytes_sum_to_redis", apacheLogBytesSumToRedis.keyValPairOutput, redisOutTotalSumBytes.input);
    dag.addStream("apache_log_dimension_counter_to_redis", topNCountToRedis.keyValueMapOutput, redisOutTopNCount.input);
    dag.addStream("apache_log_dimension_sum_to_redis", topNSumToRedis.keyValueMapOutput, redisOutTopNSum.input);
    dag.addStream("apache_log_filtered_dimension_count_to_redis", filteredTopNCountToRedis.keyValueMapOutput, redisOutFilteredTopNSum.input);

    /*
     * Websocket output to UI from calculated aggregations
     */
    dag.addStream("MysqlTopAggregations", mysqlTopNOpr.top, wsOutput(dag, "mysqlTopAggrs"), mysqlConsole.input);
    dag.addStream("SyslogTopAggregations", syslogTopNOpr.top, wsOutput(dag, "syslogTopAggrs"), syslogConsole.input);
    dag.addStream("SystemData", systemLogJsonToMap.outputMap, wsOutput(dag, "systemData"), logScoreOperator.systemLogs, systemConsole.input);

  }

}

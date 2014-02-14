/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import com.datatorrent.api.*;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.Partitionable.Partition;
import com.datatorrent.api.Partitionable.PartitionKeys;

import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;

import javax.validation.constraints.NotNull;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.lib.logs.DimensionObject;
import com.datatorrent.lib.util.KryoSerializableStreamCodec;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.apps.logstream.PropertyRegistry.PropertyRegistry;
import com.datatorrent.apps.logstream.LogstreamUtil.AggregateOperation;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class DimensionOperator extends BaseOperator implements Partitionable<DimensionOperator>
{
  @NotNull
  private PropertyRegistry<String> registry;
  private static final Logger logger = LoggerFactory.getLogger(DimensionOperator.class);
  //private Map<String, Map<String, Number>> dataMap;
  private String timeKeyName;
  private long windowWidth = 500;
  private long currentWindowId;
  private transient TimeZone timeZone = TimeZone.getTimeZone("GMT");
  private transient Calendar calendar = new GregorianCalendar(timeZone);
  //protected List<String> valueKeyNames = new ArrayList<String>();
  private int timeBucketFlags;
  private Map<String, Map<String, Map<AggregateOperation, Number>>> cacheObject = new HashMap<String, Map<String, Map<AggregateOperation, Number>>>();
  private HashMap<String, Number> recordType = new HashMap<String, Number>();
  private HashMap<Integer, HashMap<String, HashSet<AggregateOperation>>> valueOperations = new HashMap<Integer, HashMap<String, HashSet<AggregateOperation>>>();
  private HashMap<Integer, ArrayList<Integer>> dimensionCombinationList = new HashMap<Integer, ArrayList<Integer>>();
  private transient boolean firstTuple = true;
  private ArrayList<Integer> dimensionCombinations;
  private HashMap<String, HashSet<AggregateOperation>> valueOperationTypes;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (context != null) {
      windowWidth = context.getValue(DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
    }
    LogstreamPropertyRegistry.setInstance(registry);
  }

  /**
   * key: timebucket|timestamp|recordtype|dimensionId|value.operationType
   * value: DimensionObject
   */
  @OutputPortFieldAnnotation(name = "aggregationsOutput")
  public final transient DefaultOutputPort<Map<String, DimensionObject<String>>> aggregationsOutput = new DefaultOutputPort<Map<String, DimensionObject<String>>>()
  {
    @Override
    public Unifier<Map<String, DimensionObject<String>>> getUnifier()
    {
      DimensionOperatorUnifier unifier = new DimensionOperatorUnifier();
      return unifier;
    }

  };
  /*
   public final transient DefaultOutputPort<Map<String, Map<String, Map<AggregateOperation, Number>>>> cacheObjOutput = new DefaultOutputPort<Map<String, Map<String, Map<AggregateOperation, Number>>>>(){

   @Override
   public Unifier<Map<String, Map<String, Map<AggregateOperation, Number>>>> getUnifier()
   {
   DimensionOperatorUnifier unifier = new DimensionOperatorUnifier();
   return unifier;
   }
   };
   */
  @InputPortFieldAnnotation(name = "in")
  public final transient DefaultInputPort<Map<String, Object>> in = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {

      //int logTypeId = (Integer)tuple.get("LOG_TYPE");
      //String lookupTypeValue = registry.lookupValue(logTypeId);
      //String lookupFilterValue = registry.lookupValue((Integer)tuple.get("FILTER"));
      //logger.info("#ashwin DIMENSION OPERATOR tuple received type = {} filter = {}", lookupTypeValue, lookupFilterValue);
      DimensionOperator.this.processTuple(tuple);
    }

    @Override
    public Class<? extends StreamCodec<Map<String, Object>>> getStreamCodec()
    {
      return DimensionOperatorStreamCodec.class;
    }

  };

  protected void processTuple(Map<String, Object> tuple)
  {

    if (firstTuple) {
      logger.info("#ashwin FIRST TUPLE type = {} filter = {}", tuple.get("LOG_TYPE"), tuple.get("FILTER"));
      // populate record type
      DimensionOperator.this.extractType(tuple);

      // create all dimension combinations if not specified by user
      if (!dimensionCombinationList.containsKey((Integer)recordType.get("LOG_TYPE"))) {
        DimensionOperator.this.createAllDimensionCombinations();
      }
      dimensionCombinations = dimensionCombinationList.get((Integer)recordType.get("LOG_TYPE"));
      valueOperationTypes = valueOperations.get((Integer)recordType.get("LOG_TYPE"));
      firstTuple = false;
    }

    Number receivedLogType = (Number)tuple.get("LOG_TYPE");
    Number receivedFilter = (Number)tuple.get("FILTER");

    Number expectedLogType = recordType.get("LOG_TYPE");
    Number expectedFilter = recordType.get("FILTER");

    if (!receivedLogType.equals(expectedLogType) || !receivedFilter.equals(expectedFilter)) {
      logger.error("Unexpected tuple");
      logger.error("expected log type = {} received = {}", expectedLogType, receivedLogType);
      logger.error("expected filter = {} received = {}", expectedFilter, receivedFilter);
    }
    else {
      List<String> timeBucketList = DimensionOperator.this.getTimeBucketList(tuple);
      for (String timeBucket : timeBucketList) {

        for (Integer dimensionCombinationId : dimensionCombinations) {
          String dimensionCombination = registry.lookupValue(dimensionCombinationId);
          String[] dimensions = dimensionCombination.split(":");

          String dimValueName = new String();
          boolean isBadTuple = false;
          if (dimensions != null) {
            for (String dimension : dimensions) {
              Object dimVal = tuple.get(dimension);
              if (dimVal == null) {
                logger.error("dimension \"{}\" not found in tuple", dimension);
                //logger.error("dimension \"{}\" not found in tuple {}", dimension, tuple);
                isBadTuple = true;
                continue;
              }
              if (!dimValueName.isEmpty()) {
                dimValueName += ",";
              }
              dimValueName += tuple.get(dimension).toString();
            }
          }

          if (!isBadTuple) {
            for (Entry<String, HashSet<AggregateOperation>> entry : valueOperationTypes.entrySet()) {
              String valueKeyName = entry.getKey();
              Object value = tuple.get(valueKeyName);
              Number numberValue = LogstreamUtil.extractNumber(value);
              DimensionOperator.this.doComputations(timeBucket, dimensionCombinationId, dimValueName, valueKeyName, numberValue);
            }
          }
        }
      }
    }
  }

  protected List<String> getTimeBucketList(Map<String, Object> tuple)
  {
    long time;
    if (timeKeyName != null) {
      time = (Long)tuple.get(timeKeyName);
    }
    else {
      time = LogstreamUtil.extractTime(currentWindowId, windowWidth);
    }

    calendar.setTimeInMillis(time);

    List<String> timeBucketList = new ArrayList<String>();

    if ((timeBucketFlags & LogstreamUtil.TIMEBUCKET_YEAR) != 0) {
      timeBucketList.add(String.format("Y|%04d", calendar.get(Calendar.YEAR)));
    }
    if ((timeBucketFlags & LogstreamUtil.TIMEBUCKET_MONTH) != 0) {
      timeBucketList.add(String.format("M|%04d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1));
    }
    if ((timeBucketFlags & LogstreamUtil.TIMEBUCKET_WEEK) != 0) {
      timeBucketList.add(String.format("W|%04d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.WEEK_OF_YEAR)));
    }
    if ((timeBucketFlags & LogstreamUtil.TIMEBUCKET_DAY) != 0) {
      timeBucketList.add(String.format("D|%04d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH)));
    }
    if ((timeBucketFlags & LogstreamUtil.TIMEBUCKET_HOUR) != 0) {
      timeBucketList.add(String.format("h|%04d%02d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY)));
    }
    if ((timeBucketFlags & LogstreamUtil.TIMEBUCKET_MINUTE) != 0) {
      timeBucketList.add(String.format("m|%04d%02d%02d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE)));
    }

    return timeBucketList;
  }

  private void doComputations(String timeBucket, Integer dimensionCombinationId, String dimValueName, String valueKeyName, Number value)
  {
    StringBuilder sb = new StringBuilder();
    sb.append(timeBucket).append("|").append(recordType.get("LOG_TYPE")).append("|").append(recordType.get("FILTER")).append("|").append(dimensionCombinationId).append("|").append(valueKeyName);

    //final key format --> timebucket|type|filter|dimId|val
    //eg: m|201311230108|1|4|10|bytes
    String key = sb.toString();
    //HashSet<AggregateOperation> operations = valueOperationTypes.get(valueKeyNames.get(field));

    /*
     * calculate sum of field for the dimension key combination

     if (operations.contains(AggregateOperation.SUM)) {
     String finalKey = key + "|" + valueKeyNames.get(field) + "." + AggregateOperation.SUM;
     DimensionObject<String> dimObj;
     if ((dimObj = outputAggregationsObject.get(finalKey)) != null) {
     dimObj.getCount().add(value);
     }
     else {
     dimObj = new DimensionObject<String>(new MutableDouble(value), dimValueName);
     outputAggregationsObject.put(finalKey, dimObj);
     }
     }

     */

    /*
     * calculate count of field for the dimension key combination

     if (operations.contains(AggregateOperation.COUNT)) {
     String finalKey = key + "|" + valueKeyNames.get(field) + "." + AggregateOperation.COUNT;
     DimensionObject<String> dimObj;
     if ((dimObj = outputAggregationsObject.get(finalKey)) != null) {
     dimObj.getCount().add(1);
     }
     else {
     dimObj = new DimensionObject<String>(new MutableDouble(value), dimValueName);
     outputAggregationsObject.put(finalKey, dimObj);
     }
     }

     */

    /*
     * calculate agerage of field for the dimension key combination

     if (operations.contains(AggregateOperation.AVERAGE)) {
     String finalAvgKey = key + "|" + valueKeyNames.get(field) + "." + AggregateOperation.AVERAGE;
     String finalCountKey = key + "|" + valueKeyNames.get(field) + "." + AggregateOperation.COUNT;
     DimensionObject<String> dimObj;

     if ((dimObj = outputAggregationsObject.get(finalAvgKey)) != null) {
     double avg = dimObj.getCount().doubleValue();
     double count = outputAggregationsObject.get(finalCountKey).getCount().doubleValue();
     double newAvg = ((avg * (count - 1)) + value.doubleValue()) / count;
     dimObj.setCount(new MutableDouble(newAvg));
     }
     else {
     dimObj = new DimensionObject<String>(new MutableDouble(value), dimValueName);
     outputAggregationsObject.put(finalAvgKey, dimObj);
     }
     }
     */



    Map<AggregateOperation, Number> aggregations;


    if (cacheObject.containsKey(key)) {
      Map<String, Map<AggregateOperation, Number>> dimValueNames = cacheObject.get(key);
      if (dimValueNames.containsKey(dimValueName)) {
        aggregations = dimValueNames.get(dimValueName);
      }
      else {
        aggregations = new HashMap<AggregateOperation, Number>();
        for (AggregateOperation aggregationType : valueOperationTypes.get(valueKeyName)) {
          aggregations.put(aggregationType, new MutableDouble(0));
        }

        dimValueNames.put(dimValueName, aggregations);
      }
    }
    else {
      Map<String, Map<AggregateOperation, Number>> newDimValueNames = new HashMap<String, Map<AggregateOperation, Number>>();
      aggregations = new HashMap<AggregateOperation, Number>();
      for (AggregateOperation aggregationType : valueOperationTypes.get(valueKeyName)) {
        aggregations.put(aggregationType, new MutableDouble(0));
      }
      newDimValueNames.put(dimValueName, aggregations);
      cacheObject.put(key, newDimValueNames);
    }

    if (aggregations.containsKey(AggregateOperation.SUM)) {
      MutableDouble aggrVal = (MutableDouble)aggregations.get(AggregateOperation.SUM);
      aggrVal.add(value);
    }

    if (aggregations.containsKey(AggregateOperation.COUNT)) {
      MutableDouble aggrVal = (MutableDouble)aggregations.get(AggregateOperation.COUNT);
      aggrVal.add(1);
    }

    if (aggregations.containsKey(AggregateOperation.AVERAGE)) {
      double avgVal = aggregations.get(AggregateOperation.AVERAGE).doubleValue();
      double countVal = aggregations.get(AggregateOperation.COUNT).doubleValue();
      double newAvg = ((avgVal * (countVal - 1)) + value.doubleValue()) / countVal;
      aggregations.put(AggregateOperation.AVERAGE, new MutableDouble(newAvg));
    }

  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    currentWindowId = windowId;
    //dataMap = new HashMap<String, Map<String, Number>>();
  }

  @Override
  public void endWindow()
  {
    //logger.info("cache object size = {}", cacheObject.size());
    HashMap<String, DimensionObject<String>> outputAggregationsObject;
    for (Entry<String, Map<String, Map<AggregateOperation, Number>>> keys : cacheObject.entrySet()) {
      String key = keys.getKey();
      Map<String, Map<AggregateOperation, Number>> dimValues = keys.getValue();

      for (Entry<String, Map<AggregateOperation, Number>> dimValue : dimValues.entrySet()) {
        String dimValueName = dimValue.getKey();
        Map<AggregateOperation, Number> operations = dimValue.getValue();

        outputAggregationsObject = new HashMap<String, DimensionObject<String>>();

        for (Entry<AggregateOperation, Number> operation : operations.entrySet()) {
          AggregateOperation aggrOperationType = operation.getKey();
          Number aggr = operation.getValue();

          String outKey = key + "." + aggrOperationType.name();
          DimensionObject<String> outDimObj = new DimensionObject<String>((MutableDouble)aggr, dimValueName);

          outputAggregationsObject.put(outKey, outDimObj);

        }
        //logger.info("emitting tuple...{}", outputAggregationsObject);
        aggregationsOutput.emit(outputAggregationsObject);
      }

    }
    /*
     if (outputAggregationsObject != null) {
     for (Entry<String, ArrayList<DimensionObject<String>>> aggregations : outputAggregationsObject.entrySet()) {
     String key = aggregations.getKey();
     ArrayList<DimensionObject<String>> dimObjList = aggregations.getValue();

     for (DimensionObject<String> dimObj : dimObjList) {
     HashMap<String, DimensionObject<String>> outObj = new HashMap<String, DimensionObject<String>>();
     outObj.put(key, dimObj);
     aggregationsOutput.emit(outObj);
     }
     }
     }
     */

    /*
     outputAggregationsObject = new HashMap<String, DimensionObject<String>>();

     for (Entry<String, Map<String, Map<AggregateOperation, Number>>> keys : cacheObject.entrySet()) {
     String key = keys.getKey();
     Map<String, Map<AggregateOperation, Number>> dimValues = keys.getValue();

     for (Entry<String, Map<AggregateOperation, Number>> dimValue : dimValues.entrySet()) {
     String dimValueName = dimValue.getKey();
     Map<AggregateOperation, Number> operations = dimValue.getValue();

     outputAggregationsObject = new HashMap<String, DimensionObject<String>>();

     for (Entry<AggregateOperation, Number> operation : operations.entrySet()) {
     AggregateOperation aggrOperationType = operation.getKey();
     Number aggr = operation.getValue();

     String outKey = key + aggrOperationType.name();
     DimensionObject<String> outDimObj = new DimensionObject<String>((MutableDouble)aggr, dimValueName);

     outputAggregationsObject.put(outKey, outDimObj);

     }
     aggregationsOutput.emit(outputAggregationsObject);
     }

     }
     */

    /*
     if (!cacheObject.isEmpty()) {
     cacheObjOutput.emit(cacheObject);
     }
     */


    /*
     if (!dataMap.isEmpty()) {
     out.emit(dataMap);
     LOG.info("Number of keyval pairs: {}", dataMap.size());
     }
     */
  }

  public void setRegistry(PropertyRegistry<String> registry)
  {
    DimensionOperator.this.registry = registry;
  }

  public void setDimensionsFromString(String[] dimensionInputString)
  {
    ArrayList<Integer> dimCombinations = new ArrayList<Integer>();
    HashMap<String, HashSet<AggregateOperation>> valOpTypes = new HashMap<String, HashSet<AggregateOperation>>();
    String type = null;
    // user input example::
    // type=apache,timebucket=m,timebucket=h,a:b:c,b:c,b,d,values=x.sum:y.sum:y.avg
    for (String inputs : dimensionInputString) {
      String[] split = inputs.split("=", 2);
      if (split[0].toLowerCase().equals("timebucket")) {

        int timeBucket = LogstreamUtil.extractTimeBucket(split[1]);
        if (timeBucket == 0) {
          logger.error("invalid time bucket", split[1]);
        }
        timeBucketFlags |= timeBucket;
      }
      else if (split[0].toLowerCase().equals("values")) {
        String[] values = split[1].split(":");
        for (String value : values) {
          String[] valueNames = value.split("\\.");
          logger.info("value = {}, value after split = {}", value, Arrays.toString(valueNames));
          String valueName = valueNames[0];
          String valueType = valueNames[1];
          //valueKeyNames.add(valueName);
          if (valueType.toLowerCase().equals("sum")) {
            if (valOpTypes.containsKey(valueName)) {
              valOpTypes.get(valueName).add(AggregateOperation.SUM);
            }
            else {
              HashSet<AggregateOperation> valueTypeList = new HashSet<AggregateOperation>();
              valueTypeList.add(AggregateOperation.SUM);
              valOpTypes.put(valueName, valueTypeList);
            }
          }
          else if (valueType.equals("avg") || valueType.equals("average")) {
            if (valOpTypes.containsKey(valueName)) {
              valOpTypes.get(valueName).add(AggregateOperation.AVERAGE);
              valOpTypes.get(valueName).add(AggregateOperation.COUNT);
            }
            else {
              HashSet<AggregateOperation> valueTypeList = new HashSet<AggregateOperation>();
              valueTypeList.add(AggregateOperation.AVERAGE);
              valueTypeList.add(AggregateOperation.COUNT);
              valOpTypes.put(valueName, valueTypeList);
            }
          }
          else if (valueType.equals("count")) {
            if (valOpTypes.containsKey(valueName)) {
              valOpTypes.get(valueName).add(AggregateOperation.COUNT);
            }
            else {
              HashSet<AggregateOperation> valueTypeList = new HashSet<AggregateOperation>();
              valueTypeList.add(AggregateOperation.COUNT);
              valOpTypes.put(valueName, valueTypeList);
            }
          }
        }
      }
      else if (split[0].toLowerCase().equals("type")) {
        type = split[1];
      }
      else {
        // dimensions
          /*
         String[] dimensionKeys = dimension.split(":");
         Set<String> dimensionKeySet = new HashSet<String>();

         for (String dimensionKey : dimensionKeys) {
         dimensionKeySet.add(dimensionKey);
         }
         this.addCombination(dimensionKeySet);
         //int dim = registry.bind("DIMENSION", dimensionKeySet.toString());
         */
        int dim = registry.bind("DIMENSION", inputs);
        dimCombinations.add(dim);
      }
    }

    dimensionCombinationList.put(registry.getIndex("LOG_TYPE", type), dimCombinations);
    valueOperations.put(registry.getIndex("LOG_TYPE", type), valOpTypes);
  }

  @Override
  protected Object clone() throws CloneNotSupportedException
  {
    DimensionOperator dimOper = new DimensionOperator();
    dimOper.registry = DimensionOperator.this.registry;
    dimOper.timeBucketFlags = DimensionOperator.this.timeBucketFlags;
    dimOper.valueOperations = new HashMap<Integer, HashMap<String, HashSet<AggregateOperation>>>(DimensionOperator.this.valueOperations);
    dimOper.dimensionCombinationList = new HashMap<Integer, ArrayList<Integer>>(DimensionOperator.this.dimensionCombinationList);

    return dimOper;
  }

  @Override
  public Collection<Partition<DimensionOperator>> definePartitions(Collection<Partition<DimensionOperator>> partitions, int incrementalCapacity)
  {
    ArrayList<Partition<DimensionOperator>> newPartitions = new ArrayList<Partition<DimensionOperator>>();
    String[] filters = registry.list("FILTER");
    int partitionSize;

    if (partitions.size() == 1) {
      // initial partitions; functional partitioning
      partitionSize = filters.length;
    }
    else {
      // redo partitions; double the partitions
      partitionSize = partitions.size() * 2;

    }
    for (int i = 0; i < partitionSize; i++) {
      try {
        DimensionOperator dimensionOperator = (DimensionOperator)DimensionOperator.this.clone();

        Partition<DimensionOperator> partition = new DefaultPartition<DimensionOperator>(dimensionOperator);
        newPartitions.add(partition);
      }
      catch (CloneNotSupportedException ex) {
        java.util.logging.Logger.getLogger(DimensionOperator.class.getName()).log(Level.SEVERE, null, ex);
      }
    }

    int partitionBits = (Integer.numberOfLeadingZeros(0) - Integer.numberOfLeadingZeros(partitionSize / filters.length - 1));
    int partitionMask = 0;
    if (partitionBits > 0) {
      partitionMask = -1 >>> (Integer.numberOfLeadingZeros(-1)) - partitionBits;
    }

    partitionMask = (partitionMask << 16) | 0xffff; // right most 16 bits used for functional partitioning

    for (int i = 0; i < newPartitions.size(); i++) {
      Partition<DimensionOperator> partition = newPartitions.get(i);
      String partitionVal = filters[i % filters.length];
      int bits = i / filters.length;
      int filterId = registry.getIndex("FILTER", partitionVal);
      logger.info("#ashwin DIMENSION OPERATOR PARTITIONING# filterId = {}", Integer.toBinaryString(filterId));
      filterId = 0xffff & filterId; // clear out first 16 bits
      logger.info("#ashwin DIMENSION OPERATOR PARTITIONING# filterId after clearing first 16 bits = {}", Integer.toBinaryString(filterId));
      int partitionKey = (bits << 16) | filterId; // first 16 bits for dynamic partitioning, last 16 bits for functional partitioning
      logger.info("#ashwin DIMENSION OPERATOR PARTITIONING# bits = {} partitionKey = {}", Integer.toBinaryString(bits), Integer.toBinaryString(partitionKey));
      logger.info("#ashwin DIMENSION OPERATOR PARTITIONING# partitionMask = {}", Integer.toBinaryString(partitionMask));
      partition.getPartitionKeys().put(in, new PartitionKeys(partitionMask, Sets.newHashSet(partitionKey)));
    }

    return newPartitions;
  }

  public void setTimeKeyName(String timeKeyName)
  {
    this.timeKeyName = timeKeyName;
  }

  private void extractType(Map<String, Object> tuple)
  {
    recordType.put("LOG_TYPE", (Number)tuple.get("LOG_TYPE"));
    recordType.put("FILTER", (Number)tuple.get("FILTER"));

    //dataMap.put("RECORD_TYPE", recordType);
  }

  /*
   public void addValueKeyName(String key)
   {
   valueKeyNames.add(key);
   }

   public void setTimeBucketFlags(int timeBucketFlags)
   {
   this.timeBucketFlags = timeBucketFlags;
   }
   */
  private void createAllDimensionCombinations()
  {
    logger.info("need to create all dimensions for type {}", recordType.get("LOG_TYPE"));
    //TODO create all dim combinations
    // temporary code to skip null pointer
    dimensionCombinationList.put((Integer)recordType.get("LOG_TYPE"), new ArrayList<Integer>());
  }

  public static class DimensionOperatorStreamCodec extends KryoSerializableStreamCodec<Map<String, Object>>
  {
    @Override
    public int getPartition(Map<String, Object> o)
    {
      int ret = 0;
      PropertyRegistry<String> registry = LogstreamPropertyRegistry.getInstance();
      String[] list = registry.list("FILTER");
      if (list == null) {
        return 0;
      }
      else if (list.length == 0) {
        return 0;
      }

      //ret = registry.getIndex("FILTER", (String)o.get("FILTER"));
      int filterId = (Integer)o.get("FILTER");
      int hashCode = o.hashCode();

      filterId = 0xffff & filterId; // clear out first 16 bits

      ret = (hashCode << 16) | filterId; // first 16 bits represent hashcode, last 16 bits represent filter type

      //logger.info("#ashwin DIMENSION OPERATOR GETPARTITION partitionkey = {} hashcode = {} filterId = {}",Integer.toBinaryString(ret), Integer.toBinaryString(hashCode), Integer.toBinaryString(filterId));

      return ret;

    }

  }

}

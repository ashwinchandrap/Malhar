/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.apps.logstream.PropertyRegistry.PropertyRegistry;
import com.datatorrent.apps.logstream.Util.AggregateOperation;
import com.datatorrent.lib.logs.DimensionObject;
import com.datatorrent.lib.util.KryoSerializableStreamCodec;
import com.google.common.collect.Sets;
import java.util.*;
import java.util.Map.Entry;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang.mutable.MutableDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class DimensionOperator extends BaseOperator implements Partitionable<DimensionOperator>
{
  @NotNull
  private static PropertyRegistry<String> registry;
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
  private Map<String, HashSet<AggregateOperation>> valueOperationTypes = new HashMap<String, HashSet<AggregateOperation>>();
  private HashMap<Integer, ArrayList<Integer>> dimensionCombinationList = new HashMap<Integer, ArrayList<Integer>>();
  private HashMap<String, DimensionObject<String>> outputAggregationsObject = new HashMap<String, DimensionObject<String>>();
  private boolean firstTuple = true;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (context != null) {
      windowWidth = context.getValue(DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
    }
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
      DimensionOperator.this.processTuple(tuple);
    }

  };

  protected void processTuple(Map<String, Object> tuple)
  {
    if (firstTuple) {
      // populate record type
      DimensionOperator.this.extractType(tuple);

      // create all dimension combinations if not specified by user
      if (dimensionCombinationList.isEmpty()) {
        DimensionOperator.this.createAllDimensionCombinations();
      }

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
      ArrayList<Integer> dimensionCombinations = dimensionCombinationList.get((Integer)recordType.get("LOG_TYPE"));
      logger.info("dimension combinations = {}, timbucket flags = {}, time buckets = {}", dimensionCombinations, Integer.toBinaryString(timeBucketFlags), timeBucketList);
      for (String timeBucket : timeBucketList) {

        for (Integer dimensionCombinationId : dimensionCombinations) {
          String dimensionCombination = registry.lookupValue(dimensionCombinationId);
          String[] dimensions = dimensionCombination.split(":");

          String dimValueName = new String();
          if (dimensions != null) {
            for (String dimension : dimensions) {
              if (!dimValueName.isEmpty()) {
                dimValueName += ",";
              }
              dimValueName += tuple.get(dimension).toString();
            }
          }

          for (Entry<String, HashSet<AggregateOperation>> entry : valueOperationTypes.entrySet()) {
            String valueKeyName = entry.getKey();
            Object value = tuple.get(valueKeyName);
            Number numberValue = Util.extractNumber(value);
            DimensionOperator.this.doComputations(timeBucket, dimensionCombinationId, dimValueName, valueKeyName, numberValue);
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
      time = Util.extractTime(currentWindowId, windowWidth);
    }

    calendar.setTimeInMillis(time);

    List<String> timeBucketList = new ArrayList<String>();

    if ((timeBucketFlags & Util.TIMEBUCKET_YEAR) != 0) {
      timeBucketList.add(String.format("Y|%04d", calendar.get(Calendar.YEAR)));
    }
    if ((timeBucketFlags & Util.TIMEBUCKET_MONTH) != 0) {
      timeBucketList.add(String.format("M|%04d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1));
    }
    if ((timeBucketFlags & Util.TIMEBUCKET_WEEK) != 0) {
      timeBucketList.add(String.format("W|%04d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.WEEK_OF_YEAR)));
    }
    if ((timeBucketFlags & Util.TIMEBUCKET_DAY) != 0) {
      timeBucketList.add(String.format("D|%04d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH)));
    }
    if ((timeBucketFlags & Util.TIMEBUCKET_HOUR) != 0) {
      timeBucketList.add(String.format("h|%04d%02d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY)));
    }
    if ((timeBucketFlags & Util.TIMEBUCKET_MINUTE) != 0) {
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
        aggregations = new EnumMap<AggregateOperation, Number>(AggregateOperation.class);
        for (AggregateOperation aggregationType : valueOperationTypes.get(valueKeyName)) {
          aggregations.put(aggregationType, new MutableDouble(0));
        }

        dimValueNames.put(dimValueName, aggregations);
      }
    }
    else {
      Map<String, Map<AggregateOperation, Number>> newDimValueNames = new HashMap<String, Map<AggregateOperation, Number>>();
      aggregations = new EnumMap<AggregateOperation, Number>(AggregateOperation.class);
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
    this.registry = registry;
  }

  public void setDimensionsFromString(String[] dimensionInputString)
  {
    ArrayList<Integer> dimensionCombinations = new ArrayList<Integer>();
    String type = null;
    // user input example::
    // type=apache,timebucket=m,timebucket=h,a:b:c,b:c,b,d,values=x.sum:y.sum:y.avg
    for (String inputs : dimensionInputString) {
      String[] split = inputs.split("=", 2);
      if (split[0].toLowerCase().equals("timebucket")) {

        int timeBucket = Util.extractTimeBucket(split[1]);
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
            if (this.valueOperationTypes.containsKey(valueName)) {
              this.valueOperationTypes.get(valueName).add(AggregateOperation.SUM);
            }
            else {
              HashSet<AggregateOperation> valueTypeList = new HashSet<AggregateOperation>();
              valueTypeList.add(AggregateOperation.SUM);
              this.valueOperationTypes.put(valueName, valueTypeList);
            }
          }
          else if (valueType.equals("avg") || valueType.equals("average")) {
            if (this.valueOperationTypes.containsKey(valueName)) {
              this.valueOperationTypes.get(valueName).add(AggregateOperation.AVERAGE);
              this.valueOperationTypes.get(valueName).add(AggregateOperation.COUNT);
            }
            else {
              HashSet<AggregateOperation> valueTypeList = new HashSet<AggregateOperation>();
              valueTypeList.add(AggregateOperation.AVERAGE);
              valueTypeList.add(AggregateOperation.COUNT);
              this.valueOperationTypes.put(valueName, valueTypeList);
            }
          }
          else if (valueType.equals("count")) {
            if (this.valueOperationTypes.containsKey(valueName)) {
              this.valueOperationTypes.get(valueName).add(AggregateOperation.COUNT);
            }
            else {
              HashSet<AggregateOperation> valueTypeList = new HashSet<AggregateOperation>();
              valueTypeList.add(AggregateOperation.COUNT);
              this.valueOperationTypes.put(valueName, valueTypeList);
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
        dimensionCombinations.add(dim);
      }
    }

    dimensionCombinationList.put(registry.getIndex("LOG_TYPE", type), dimensionCombinations);
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
      DimensionOperator dimensionOperator = new DimensionOperator();

      Partition<DimensionOperator> partition = new DefaultPartition<DimensionOperator>(dimensionOperator);
      newPartitions.add(partition);
    }

    int partitionBits = (Integer.numberOfLeadingZeros(0) - Integer.numberOfLeadingZeros(partitionSize / filters.length - 1));
    int partitionMask = 0;
    if (partitionBits > 0) {
      partitionMask = -1 >>> (Integer.numberOfLeadingZeros(-1)) - partitionBits;
    }

    partitionMask = (partitionMask << 16) | 0xffff; // right most 16 bits used for functional partitioning

    for (int i = 0; i <= newPartitions.size(); i++) {
      Partition<DimensionOperator> partition = newPartitions.get(i);
      String partitionVal = filters[i % filters.length];
      int bits = i / filters.length;
      int filterId = registry.getIndex("FILTER", partitionVal);
      filterId = 0xffff & filterId; // clear out first 16 bits
      int partitionKey = (bits << 16) | filterId; // first 16 bits for dynamic partitioning, last 16 bits for functional partitioning
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
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public static class DimensionOperatorStreamCodec extends KryoSerializableStreamCodec<Map<String, Object>>
  {
    @Override
    public int getPartition(Map<String, Object> o)
    {
      int ret = 0;
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

      return ret;

    }

  }

}

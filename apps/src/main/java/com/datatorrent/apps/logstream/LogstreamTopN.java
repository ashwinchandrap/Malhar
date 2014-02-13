/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitionable;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.apps.logstream.PropertyRegistry.PropertyRegistry;
import com.datatorrent.lib.algo.TopN;
import com.datatorrent.lib.logs.DimensionObject;
import com.datatorrent.lib.util.KryoSerializableStreamCodec;
import com.google.common.collect.Sets;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class LogstreamTopN extends TopN<String, DimensionObject<String>> implements Partitionable<LogstreamTopN>
{
  private transient boolean firstTuple = true;
  private HashMap<String, Number> recordType = new HashMap<String, Number>();

  private static final Logger logger = LoggerFactory.getLogger(LogstreamTopN.class);
  @NotNull
  private PropertyRegistry<String> registry;


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

  @Override
  public void processTuple(Map<String, DimensionObject<String>> tuple)
  {
    //logger.info("TOPN tuple received {}", tuple);
    if (firstTuple) {
      LogstreamTopN.this.extractType(tuple);
      firstTuple = false;
    }

    Iterator<Entry<String, DimensionObject<String>>> iterator = tuple.entrySet().iterator();
    String randomKey = null;

    if (iterator.hasNext()) {
      randomKey = iterator.next().getKey();
    }

    String[] split = randomKey.split("\\|");
    Number receivedFilter = new Integer(split[3]);
    Number expectedFilter = recordType.get("FILTER");

    if (!receivedFilter.equals(expectedFilter)) {
      logger.error("Unexpected tuple");
      logger.error("expected filter = {} received = {}", expectedFilter, receivedFilter);
    }

    super.processTuple(tuple);
  }


  @Override
  protected Object clone() throws CloneNotSupportedException
  {
    LogstreamTopN logstreamTopN = new LogstreamTopN();
    logstreamTopN.registry = LogstreamTopN.this.registry;
    logstreamTopN.setN(LogstreamTopN.this.getN());

    return logstreamTopN;

  }


  @Override
  protected Class<? extends StreamCodec<Map<String, DimensionObject<String>>>> getStreamCodec()
  {
    return LogstreamTopNStreamCodec.class;
  }

  @Override
  public Collection<Partition<LogstreamTopN>> definePartitions(Collection<Partition<LogstreamTopN>> partitions, int incrementalCapacity)
  {
    ArrayList<Partition<LogstreamTopN>> newPartitions = new ArrayList<Partition<LogstreamTopN>>();
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
        LogstreamTopN logstreamTopN = (LogstreamTopN)LogstreamTopN.this.clone();

        Partition<LogstreamTopN> partition = new DefaultPartition<LogstreamTopN>(logstreamTopN);
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
      Partition<LogstreamTopN> partition = newPartitions.get(i);
      String partitionVal = filters[i % filters.length];
      int bits = i / filters.length;
      int filterId = registry.getIndex("FILTER", partitionVal);
      logger.info("#ashwin TOPN OPERATOR PARTITIONING# filterId = {}", Integer.toBinaryString(filterId));
      filterId = 0xffff & filterId; // clear out first 16 bits
      logger.info("#ashwin TOPN OPERATOR PARTITIONING# filterId after clearing first 16 bits = {}", Integer.toBinaryString(filterId));
      int partitionKey = (bits << 16) | filterId; // first 16 bits for dynamic partitioning, last 16 bits for functional partitioning
      logger.info("#ashwin TOPN OPERATOR PARTITIONING# bits = {} partitionKey = {}", Integer.toBinaryString(bits), Integer.toBinaryString(partitionKey));
      logger.info("#ashwin TOPN OPERATOR PARTITIONING# partitionMask = {}", Integer.toBinaryString(partitionMask));
      partition.getPartitionKeys().put(data, new PartitionKeys(partitionMask, Sets.newHashSet(partitionKey)));
    }

    return newPartitions;

  }

  private void extractType(Map<String, DimensionObject<String>> tuple)
  {
    Iterator<Entry<String, DimensionObject<String>>> iterator = tuple.entrySet().iterator();
    String randomKey = null;

    if (iterator.hasNext()) {
      randomKey = iterator.next().getKey();
    }
    String[] split = randomKey.split("\\|");
    Number filterId = new Integer(split[3]);

    recordType.put("FILTER", filterId);

  }

  public static class LogstreamTopNStreamCodec extends KryoSerializableStreamCodec<Map<String, DimensionObject<String>>>
  {
    @Override
    public int getPartition(Map<String, DimensionObject<String>> t)
    {
      Iterator<String> iterator = t.keySet().iterator();
      String key = iterator.next();
      String[] split = key.split("\\|");
      int filterId = new Integer(split[3]); // filter id location in input record key

      int ret = 0;
      PropertyRegistry<String> registry = LogstreamPropertyRegistry.getInstance();
      String[] list = registry.list("FILTER");
      if (list == null) {
        return 0;
      }
      else if (list.length == 0) {
        return 0;
      }

      int hashCode = t.hashCode();

      filterId = 0xffff & filterId; // clear out first 16 bits

      ret = (hashCode << 16) | filterId; // first 16 bits represent hashcode, last 16 bits represent filter type

      //logger.info("#ashwin TOPN OPERATOR GETPARTITION partitionkey = {} hashcode = {} filterId = {}",Integer.toBinaryString(ret), Integer.toBinaryString(hashCode), Integer.toBinaryString(filterId));

      return ret;

    }

  }
}
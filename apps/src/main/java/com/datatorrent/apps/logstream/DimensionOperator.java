/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import com.datatorrent.api.Partitionable;
import com.datatorrent.apps.logstream.PropertyRegistry.PropertyRegistry;
import static com.datatorrent.lib.util.DimensionTimeBucketOperator.*;
import com.datatorrent.lib.util.DimensionTimeBucketSumOperator;
import com.datatorrent.lib.util.KryoSerializableStreamCodec;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class DimensionOperator extends DimensionTimeBucketSumOperator implements Partitionable<DimensionOperator>
{
  private transient static PropertyRegistry<String> registry;

  public void setRegistry(PropertyRegistry<String> registry)
  {
    this.registry = registry;
  }

  public void addFieldList(String[] fields)
  {
    for (String field : fields) {
      this.addDimensionKeyName(field);
    }
  }

  public void addDimensionsFromString(String[] dimensions)
  {
    // user input example::
    // timebucket=m,timebucket=h,a:b:c,b:c,b,d,values=x.sum,y.sum,y.avg
    int timeBucketflags = 0;
    try {
      for (String dimension : dimensions) {
        String[] split = dimension.split("=", 2);
        if (split[0].toLowerCase().equals("timebucket")) {

          int timeBucket = 0;
          if (split[1].equals("m")) {
            timeBucket = TIMEBUCKET_MINUTE;
          }
          else if (split[1].equals("h")) {
            timeBucket = TIMEBUCKET_HOUR;
          }
          else if (split[1].equals("D")) {
            timeBucket = TIMEBUCKET_DAY;
          }
          else if (split[1].equals("W")) {
            timeBucket = TIMEBUCKET_WEEK;
          }
          else if (split[1].equals("M")) {
            timeBucket = TIMEBUCKET_MONTH;
          }
          else if (split[1].equals("Y")) {
            timeBucket = TIMEBUCKET_YEAR;
          }
          timeBucketflags |= timeBucket;
        }
        else if (split[0].toLowerCase().equals("values")) {
          String[] values = split[1].split(":");
          for (String value : values) {
            String[] valueNames = value.split(".");
            for (String valueName : valueNames) {
              this.addValueKeyName(valueName);
            }
          }
        }
        else {
          String[] dimensionKeys = dimension.split(":");
          Set<String> dimensionKeySet = new HashSet<String>();

          for (String dimensionKey : dimensionKeys) {
            dimensionKeySet.add(dimensionKey);
          }
          this.addCombination(dimensionKeySet);
        }

      }

      this.setTimeBucketFlags(timeBucketflags);
    }
    catch (NoSuchFieldException ex) {
      Logger.getLogger(DimensionOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public Collection<Partition<DimensionOperator>> definePartitions(Collection<Partition<DimensionOperator>> clctn, int i)
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public static class DimensionOperatorStreamCodec extends KryoSerializableStreamCodec<Map<String, Object>>
  {
    @Override
    public int getPartition(Map<String, Object> o)
    {
      int ret;
      String[] list = registry.list("FILTER");
      if (list == null) {
        return 0;
      }
      else if (list.length == 0) {
        return 0;
      }

      ret = registry.getIndex("FILTER", (String)o.get("FILTER"));
      return ret;

    }

  }


}

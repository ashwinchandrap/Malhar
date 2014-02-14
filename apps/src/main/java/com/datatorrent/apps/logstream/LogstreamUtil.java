/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Map;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class LogstreamUtil
{
  public static final int TIMEBUCKET_MINUTE = 1;
  public static final int TIMEBUCKET_HOUR = 2;
  public static final int TIMEBUCKET_DAY = 4;
  public static final int TIMEBUCKET_WEEK = 8;
  public static final int TIMEBUCKET_MONTH = 16;
  public static final int TIMEBUCKET_YEAR = 32;

  public enum AggregateOperation
  {
    SUM, AVERAGE, COUNT
  };

  public static long extractTime(long currentWindowId, long windowWidth)
  {
    long time;
    time = (currentWindowId >>> 32) * 1000 + windowWidth * (currentWindowId & 0xffffffffL);
    return time;
  }

  public static Number extractNumber(Object value)
  {
    NumberFormat numberFormat = NumberFormat.getInstance();
    if (value instanceof Number) {
      return (Number)value;
    }
    else if (value == null) {
      return new Long(0);
    }
    else {
      try {
        return numberFormat.parse(value.toString());
      }
      catch (ParseException ex) {
      }
    }
    return new Long(0);
  }

  public static int extractTimeBucket(String bucket) {
    int timeBucket = 0;
    if (bucket.equals("m")) {
      timeBucket = TIMEBUCKET_MINUTE;
    }
    else if (bucket.equals("h")) {
      timeBucket = TIMEBUCKET_HOUR;
    }
    else if (bucket.equals("D")) {
      timeBucket = TIMEBUCKET_DAY;
    }
    else if (bucket.equals("W")) {
      timeBucket = TIMEBUCKET_WEEK;
    }
    else if (bucket.equals("M")) {
      timeBucket = TIMEBUCKET_MONTH;
    }
    else if (bucket.equals("Y")) {
      timeBucket = TIMEBUCKET_YEAR;
    }

    return timeBucket;

  }



}

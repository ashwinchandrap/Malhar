/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream.PropertyRegistry;

import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.apache.activemq.util.LRUCache;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class LogstreamPropertyRegistry implements PropertyRegistry<String>
{
  private HashMap<Integer, ArrayList<String>> valueList = new HashMap<Integer, ArrayList<String>>();
  private ArrayList<String> nameList = new ArrayList<String>();
  private HashMap<String, Integer> indexMap = new HashMap<String, Integer>();

  @Override
  public synchronized int bind(String name, String value)
  {
    ArrayList<String> values = null;
    int nameIndex = nameList.indexOf(name);
    int valueIndex;

    if (nameIndex < 0) {
      nameList.add(name);
      nameIndex = nameList.indexOf(name);

      values = new ArrayList<String>();
      values.add(value);
      valueList.put(nameIndex, values);

      valueIndex = values.indexOf(value);
    }
    else {
      values = valueList.get(nameIndex);
      valueIndex = values.indexOf(value);
      if (valueIndex < 0) {
        values.add(value);
        valueIndex = values.indexOf(value);
      }
    }

    // first 16 characters represent the name, last 16 characters represent the value
    // there can be total of 2 ^ 16 names and 2 ^ 16 values for each name
    int index = nameIndex << 16 | valueIndex;

    indexMap.put(name + "_" + value, index);
    System.out.println("name value = " + name + "_" + value);
    System.out.println("name index = " + nameIndex);
    System.out.println("value index = " + valueIndex);
    System.out.println("index = " + index);

    return index;
  }

  @Override
  public String lookupValue(int index)
  {
    int nameIndex = index >> 16;
    int valueIndex = (-1 >>> -16) & index;
    ArrayList<String> values = valueList.get(nameIndex);
    String value = values.get(valueIndex);

    return value;
  }

  @Override
  public String[] list(String name)
  {
    int nameIndex = nameList.indexOf(name);

    if (nameIndex < 0) {
      return null;
    }

    ArrayList<String> values = valueList.get(nameIndex);

    return (String[])values.toArray();

  }

  @Override
  public String lookupName(int index)
  {
    int nameIndex = index >> 16;
    String name = nameList.get(nameIndex);

    return name;
  }

  @Override
  public int getIndex(String name, String value)
  {
    Integer index = indexMap.get(name + "_" + value);
    if (index == null) {
      return -1;
    } else {
      return index;
    }
  }

  @Override
  public String toString()
  {
    return indexMap.toString();
  }

}

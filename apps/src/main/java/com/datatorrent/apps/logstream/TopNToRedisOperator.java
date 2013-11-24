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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.logs.DimensionObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class TopNToRedisOperator<K, V> extends BaseOperator
{
  private HashMap<String, Integer> dimensionToDbIndexMap;

  public HashMap<String, Integer> getDimensionToDbIndexMap()
  {
    return dimensionToDbIndexMap;
  }

  public void setDimensionToDbIndexMap(HashMap<String, Integer> dimensionTodbIndexMap)
  {
    this.dimensionToDbIndexMap = dimensionTodbIndexMap;
  }

  @InputPortFieldAnnotation(name = "input")
  public final transient DefaultInputPort<HashMap<String, ArrayList<DimensionObject<String>>>> input = new DefaultInputPort<HashMap<String, ArrayList<DimensionObject<String>>>>()
  {
    @Override
    public void process(HashMap<String, ArrayList<DimensionObject<String>>> tuple)
    {
      //HashMap<String, ArrayList<DimensionObject<String>>>
      for (String dimensionKey : tuple.keySet()) {
        Integer dbIndex = dimensionToDbIndexMap.get(dimensionKey);
        if (dbIndex != null) {
          // set dbindex
          ArrayList<DimensionObject<String>> topList = tuple.get(dimensionKey);
          int numOuts = 0;
          System.out.println("\ndbindex = " + dbIndex + "\n");
          for (DimensionObject<String> item : topList) {
            Map<String, String> out = new HashMap<String, String>();
            String key = new StringBuilder(dbIndex.toString()).append("##").append(numOuts++).toString();
            String value = new StringBuilder(item.getVal()).append("##").append(item.getCount()).toString();
            //out.put(numOuts++, value);
            out.put(key, value);
            outport.emit(out);
          }
        }
      }

    }

  };
  @OutputPortFieldAnnotation(name = "output")
  public final transient DefaultOutputPort<Map<String, String>> outport = new DefaultOutputPort<Map<String, String>>();
}

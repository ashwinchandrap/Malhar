/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.stream;

import java.io.IOException;
import java.util.HashMap;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link JsonMapperOperator}
 */
public class JsonMapperOperatorTest
{
  @Test
  public void testJsonStringToHashMap() throws IOException
  {
    JsonMapperOperator<String, MyPojo> oper = new JsonMapperOperator<String, MyPojo>();

    oper.setup(null);
    CollectorTestSink objectSink = new CollectorTestSink();
    oper.output.setSink(objectSink);

    ObjectMapper mapper = new ObjectMapper(new JsonFactory());

    oper.beginWindow(0);

    MyPojo pojo1 = new MyPojo();
    pojo1.setName("country");
    pojo1.setValue("usa");

    // input test json string
    String inputJson = mapper.writeValueAsString(pojo1);

    // run the operator for the same string 1000 times
    int numtuples = 1000;
    for (int i = 0; i < numtuples; i++) {
      oper.input.process(inputJson);
    }

    oper.endWindow();

    // assert that the number of the tuples operator generates is 1000
    Assert.assertEquals("number emitted tuples", numtuples, objectSink.collectedTuples.size());

    // assert that value for one of the keys in any one of the objects from mapSink is as expected
    HashMap<String, String> obj = (HashMap<String, String>)objectSink.collectedTuples.get(510);
    Assert.assertEquals("emitted tuple", "country", obj.get("name"));
    Assert.assertEquals("emitted tuple", "usa", obj.get("value"));
  }

  @Test
  public void testJsonStringToList() throws IOException
  {
    JsonMapperOperator<String, List<Long>> oper = new JsonMapperOperator<String, List<Long>>();

    oper.setOutputClassName("java.util.List");
    oper.setup(null);
    CollectorTestSink objectSink = new CollectorTestSink();
    oper.output.setSink(objectSink);

    ObjectMapper mapper = new ObjectMapper(new JsonFactory());

    oper.beginWindow(0);

    List<Long> list = Lists.newArrayList();
    list.add(1L);
    list.add(2L);
    list.add(Long.MAX_VALUE);

    // input test json string
    String inputJson = mapper.writeValueAsString(list);

    // run the operator for the same string 1000 times
    int numtuples = 1000;
    for (int i = 0; i < numtuples; i++) {
      oper.input.process(inputJson);
    }

    oper.endWindow();

    System.out.println(objectSink.collectedTuples);
    System.out.println("long max = " + Long.MAX_VALUE);
    // assert that the number of the tuples operator generates is 1000
    Assert.assertEquals("number emitted tuples", numtuples, objectSink.collectedTuples.size());

    // assert that value for one of the keys in any one of the objects from mapSink is as expected
    List<Number> obj = (List<Number>)objectSink.collectedTuples.get(510);
    Assert.assertEquals("emitted tuple", 1L, obj.get(0).longValue());
    Assert.assertEquals("emitted tuple", 2L, obj.get(1).longValue());
    Assert.assertEquals("emitted tuple", Long.MAX_VALUE , obj.get(2).longValue());
    Assert.assertEquals("emitted tuple", 3, obj.size());
  }

  @Test
  public void testJsonByteArrayToHashMap() throws IOException
  {
    JsonMapperOperator<byte[], MyPojo> oper = new JsonMapperOperator<byte[], MyPojo>();

    oper.setup(null);
    CollectorTestSink objectSink = new CollectorTestSink();
    oper.output.setSink(objectSink);

    ObjectMapper mapper = new ObjectMapper(new JsonFactory());

    oper.beginWindow(0);

    MyPojo pojo1 = new MyPojo();
    pojo1.setName("country");
    pojo1.setValue("usa");

    // input test json string
    byte[] inputJson = mapper.writeValueAsString(pojo1).getBytes();

    // run the operator for the same string 1000 times
    int numtuples = 1000;
    for (int i = 0; i < numtuples; i++) {
      oper.input.process(inputJson);
    }

    oper.endWindow();

    // assert that the number of the tuples operator generates is 1000
    Assert.assertEquals("number emitted tuples", numtuples, objectSink.collectedTuples.size());

    // assert that value for one of the keys in any one of the objects from mapSink is as expected
    HashMap<String, String> obj = (HashMap<String, String>)objectSink.collectedTuples.get(510);
    Assert.assertEquals("emitted tuple", "country", obj.get("name"));
    Assert.assertEquals("emitted tuple", "usa", obj.get("value"));
  }

  @Test
  public void testJsonToObject() throws IOException
  {
    JsonMapperOperator<String, MyPojo> oper = new JsonMapperOperator<String, MyPojo>();

    oper.setOutputClassName("com.datatorrent.lib.stream.JsonMapperOperatorTest$MyPojo");

    oper.setup(null);
    CollectorTestSink objectSink = new CollectorTestSink();
    oper.output.setSink(objectSink);

    ObjectMapper mapper = new ObjectMapper(new JsonFactory());

    oper.beginWindow(0);

    MyPojo pojo1 = new MyPojo();
    pojo1.setName("country");
    pojo1.setValue("usa");

    // input test json string
    String inputJson = mapper.writeValueAsString(pojo1);

    // run the operator for the same string 1000 times
    int numtuples = 1000;
    for (int i = 0; i < numtuples; i++) {
      oper.input.process(inputJson);
    }

    oper.endWindow();

    // assert that the number of the operator generates is 1000
    Assert.assertEquals("number emitted tuples", numtuples, objectSink.collectedTuples.size());

    // assert that value for one of the keys in any one of the objects from mapSink is as expected

    MyPojo obj = (MyPojo)objectSink.collectedTuples.get(510);
    Assert.assertEquals("emitted tuple", "country", obj.getName());
    Assert.assertEquals("emitted tuple", "usa", obj.getValue());

  }

  @Test
  public void testObjectToJson() throws IOException
  {
    JsonMapperOperator<MyPojo, String> oper = new JsonMapperOperator<MyPojo, String>();
    oper.setup(null);

    CollectorTestSink jsonStringSink = new CollectorTestSink();
    oper.output.setSink(jsonStringSink);

    ObjectMapper mapper = new ObjectMapper(new JsonFactory());

    oper.beginWindow(0);

    MyPojo pojo = new MyPojo();
    pojo.setName("city");
    pojo.setValue("sunnyvale");

    // run the operator for the same string 1000 times
    int numtuples = 1000;
    for (int i = 0; i < numtuples; i++) {
      oper.input.process(pojo);
    }

    oper.endWindow();

    // assert that the number of the operator generates is 1000
    Assert.assertEquals("number emitted tuples", numtuples, jsonStringSink.collectedTuples.size());

    // assert that value for one of the keys in any one of the objects from mapSink is as expected
    String jsonStr = (String)jsonStringSink.collectedTuples.get(510);
    MyPojo convertedValue = mapper.readValue(jsonStr, MyPojo.class);

    Assert.assertEquals("emitted tuple", "city", convertedValue.getName());
    Assert.assertEquals("emitted tuple", "sunnyvale", convertedValue.getValue());
  }

  public static class MyPojo
  {
    String name;
    String value;

    public MyPojo()
    {
    }

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public String getValue()
    {
      return value;
    }

    public void setValue(String value)
    {
      this.value = value;
    }

    @Override
    public String toString()
    {
      return "MyPojo{" + "name=" + name + ", value=" + value + '}';
    }

  }

}

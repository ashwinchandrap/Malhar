/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.stream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.testbench.CollectorTestSink;
import java.io.IOException;
import java.util.HashMap;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class JsonMapperOperatorTest
{
  @Test
  public void testJsonToHashMap() throws IOException
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

    // assert that the number of the operator generates is 1000
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

/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.db;

import com.datatorrent.contrib.mongodb.MongoDBAggregateWriter;
import com.datatorrent.contrib.mongodb.MongoDBAggregateWriter.Aggregate;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class DataStoreOutputOperatorTest
{
  @Test
  public void testMongoDbOutput() {
    final String tableName = "aggregates2";
    MongoDBAggregateWriter dataStore = new MongoDBAggregateWriter();

    dataStore.setHostName("localhost");
    dataStore.setDataBase("testComputations");
    dataStore.setTable(tableName);
    //dataStore.setup(new OperatorContextTestHelper.TestIdOperatorContext(1));

    DataStoreOutputOperator<Aggregate> oper = new DataStoreOutputOperator<Aggregate>();

    oper.setDataStore(dataStore);

    oper.setup(null);
    dataStore.dropTable(tableName);

    oper.beginWindow(1);

    Set<String> dimensions = new HashSet<String>();
    dimensions.add("dim1");
    dimensions.add("dim2");

    Aggregate map = new Aggregate(dimensions);
    map.put("dim1", "dim11val");
    map.put("dim2", "dim21val");
    map.put("aggr1", "aggr1val");
    oper.input.process(map);

    dimensions.clear();
    dimensions.add("dim1");
    dimensions.add("dim2");
    dimensions.add("dim3");

    map = new Aggregate(dimensions);
    map.put("dim1", "dim1val");
    map.put("aggr1", "aggr12val");
    map.put("aggr2", "aggr22val");
    map.put("aggr3", "aggr32val");
    oper.input.process(map);

    oper.endWindow();


    //DBCursor cursor = oper.dataStore.db.getCollection(table).find();
    //while (cursor.hasNext()) {
    //  System.out.println(cursor.next());
    //}

    System.out.println(dataStore.find(tableName));

    oper.teardown();

  }

}

/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.db;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.lib.database.DataStoreWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotNull;

/**
 *
 * @param <T>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class DataStoreOutputOperator<T> extends BaseOperator
{
  @NotNull
  DataStoreWriter<T> dataStore;
  private List<T> cache = new ArrayList<T>();
  private long currentWindowId;

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      //dataStore.insert(t);
      processTuple(t);
    }
  };

  //public void setTable(String tableName)
  //{
  //  this.tableName = tableName;
  //}

  public void setDataStore(DataStoreWriter<T> dataStore)
  {
    this.dataStore = dataStore;
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      dataStore.connect();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      dataStore.disconnect();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void processTuple(T t) {
    cache.add(t);
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    super.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    dataStore.batchInsert(cache, currentWindowId);
  }
}

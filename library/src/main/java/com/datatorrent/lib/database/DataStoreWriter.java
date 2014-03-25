/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.database;

import com.datatorrent.lib.db.Connectable;
import java.util.List;

/**
 *
 * @param <T>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public interface DataStoreWriter<T> extends Connectable
{
  public void batchInsert(List<T> tupleList, long windowId);
}

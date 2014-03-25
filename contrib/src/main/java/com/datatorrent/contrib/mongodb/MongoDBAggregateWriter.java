/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.mongodb;

import com.datatorrent.contrib.mongodb.MongoDBAggregateWriter.Aggregate;
import com.datatorrent.contrib.redis.RedisStore;
import com.datatorrent.lib.database.DataStoreWriter;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map.Entry;
import java.util.*;
import javax.annotation.Nonnull;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class MongoDBAggregateWriter extends AbstractMongoDBConnectable implements DataStoreWriter<Aggregate>
{
  protected transient HashMap<String, BasicDBObject> tableToDocument = new HashMap<String, BasicDBObject>(); // each table has one document to insert
  protected transient HashMap<String, List<DBObject>> tableToDocumentList = new HashMap<String, List<DBObject>>();
  protected transient int tupleId;
  protected String table;
  protected long lastInsertedWindowId;

  @Override
  public void batchInsert(List<Aggregate> tupleList, long windowId)
  {
    if (windowId > lastInsertedWindowId) {
      ArrayList<DBObject> docList = new ArrayList<DBObject>();
      for (Aggregate tuple : tupleList) {
        BasicDBObject doc = new BasicDBObject();

        // add dimensions, map may not contain values for all dimensions - so make sure all dimensions are added
        for (String dimension : tuple.dimensions) {
          doc.put(dimension, tuple.get(dimension));
        }

        // add all aggregates
        for (Map.Entry<String, Object> entry : tuple.entrySet()) {
          if (!tuple.dimensions.contains(entry.getKey())) {
            doc.put(entry.getKey(), entry.getValue());
          }
        }
        docList.add(doc);
      }
      db.getCollection(table).insert(docList);
      lastInsertedWindowId = windowId;
    } else {
      logger.debug("Already inserted records for window id {}", windowId);
    }
  }

  @SuppressWarnings("rawtypes")
  public List<Map> find(String table) {
    DBCursor cursor = db.getCollection(table).find();
    DBObject next;
    ArrayList<Map> result = new ArrayList<Map>();
    while (cursor.hasNext()) {
      next = cursor.next();
      result.add(next.toMap());
    }
    return result;
  }

  public void dropTable(String table)
  {
    db.getCollection(table).drop();
  }

  @Override
  public void disconnect() throws IOException
  {
    for (Entry<String, List<DBObject>> entry : tableToDocumentList.entrySet()) {
      String table = entry.getKey();
      List<DBObject> docList = entry.getValue();
      db.getCollection(table).insert(docList);
    }

    super.disconnect();
  }

  public void setTable(String table)
  {
    this.table = table;
  }

  public static class Aggregate extends HashMap<String, Object>
  {
    @Nonnull
    Set<String> dimensions;

    public Aggregate(Set<String> dimensions)
    {
      this.dimensions = Preconditions.checkNotNull(dimensions, "dimensions");
    }

    @Override
    public boolean equals(Object o)
    {
      if (null == o || o.getClass() != this.getClass()) {
        return false;
      }
      Aggregate other = (Aggregate)o;
      if (!Objects.equal(dimensions, ((Aggregate)o).dimensions)) {
        return false;
      }
      for (String aDimension : dimensions) {
        if (!Objects.equal(get(aDimension), other.get(aDimension))) {
          return false;
        }
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      int hash = 5 ^ dimensions.hashCode() ^ Aggregate.class.hashCode();

      for (String aDimension : dimensions) {
        Object dimensionVal = get(aDimension);
        hash = 61 * hash + (dimensionVal != null ? dimensionVal.hashCode() : 0);
      }
      return hash;
    }

    @Override
    public String toString()
    {
      return super.toString();
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(RedisStore.class);
}

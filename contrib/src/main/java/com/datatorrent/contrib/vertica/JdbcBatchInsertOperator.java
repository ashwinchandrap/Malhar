/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.vertica;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.db.jdbc.JdbcStore;

import com.datatorrent.api.Context.OperatorContext;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.contrib.vertica.Batch;
import java.sql.*;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.logging.Level;

/**
 *
 * @param <FILEMETA>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class JdbcBatchInsertOperator<FILEMETA extends FileMeta> extends AbstractNonBlockingBatchWriter<FILEMETA, FILEMETA, Batch>
{
  protected transient FileSystem fs;
  @NotNull
  protected String filePath;
  private transient long fsRetryWaitMillis = 1000L;
  private transient int fsRetryAttempts = 3;
  private String delimiter = "\\|";
  private Map<String, Batch> partialBatchesHolder = Maps.newHashMap(); // holds partial new partial batches to be held temporarily generated batches are committed.
  private JdbcStore store = new JdbcStore();
  private transient Map<String, TableMeta> tables; // tableName --> tableName meta
  @NotNull
  private transient TableMetaParser parser;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    try {
      fs = FileSystem.newInstance((new Path(filePath)).toUri(), new Configuration());
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    store.connect();
    try {
      store.getConnection().setAutoCommit(false);
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

    tables = parser.extractTableMeta();
  }

  @Override
  protected FILEMETA convertInputTuple(FILEMETA input)
  {
    return input;
  }

  @Override
  protected List<Batch> generateBatches(FileMeta queueTuple)
  {
    List<String[]> newRows = fetchRows(queueTuple);
    Batch existingPartialBatch = partialBatches.get(queueTuple.tableName);

    List<String[]> rows;
    if (existingPartialBatch != null) {
      rows = Lists.newArrayList();
      rows.addAll(existingPartialBatch.rows);
      rows.addAll(newRows);
    }
    else {
      rows = newRows;
    }
    List<Batch> insertBatches = Lists.newArrayList();

    int batchStart = 0;
    while (batchStart < rows.size()) {
      int batchEnd;
      if (rows.size() - batchStart < batchSize) {
        batchEnd = rows.size();
        Batch newPartialBatch = new Batch();
        newPartialBatch.tableName = queueTuple.tableName;
        newPartialBatch.rows = rows.subList(batchStart, batchEnd);
        partialBatchesHolder.put(queueTuple.tableName, newPartialBatch);
      }
      else {
        batchEnd = batchStart + batchSize;
        Batch batch = new Batch();
        batch.rows = rows.subList(batchStart, batchEnd);
        batch.tableName = queueTuple.tableName;

        insertBatches.add(batch);
      }
      batchStart = batchEnd;
    }

    return insertBatches;
  }

  /*
   * Used to update the partial batch to the last saved partial batch. Useful in case of recovery when partial batch is generated but
   * the batches before partial batch are not processed yet before operator failure.
   */
  @Override
  protected void processedCommittedData() {
    for (Entry<String, Batch> entry : partialBatchesHolder.entrySet()) {
      String string = entry.getKey();
      Batch batch = entry.getValue();

      partialBatches.put(string, batch);
    }
    partialBatchesHolder.clear();
  }

  @Override
  protected void executeBatch(Batch batch)
  {
    PreparedStatement stmt = null;
    Savepoint savepoint = null;
    try {
      String insertSql = tables.get(batch.tableName).insertSql;
      logger.debug("insert sql = {}", insertSql);
      savepoint = store.getConnection().setSavepoint();
      stmt = store.getConnection().prepareStatement(insertSql);

      logger.debug("prepared statement = {}", stmt);

      for (String[] strings : batch.rows) {
        for (int i = 0; i < strings.length; i++) {
          stmt.setObject(i + 1, strings[i]);
        }

        stmt.addBatch();
      }

      stmt.executeBatch();
      store.getConnection().commit();
    }
    catch (BatchUpdateException batchEx) {
      logger.error("batch update exception: ", batchEx);
      try {
        store.getConnection().rollback(savepoint);
      }
      catch (SQLException sqlEx) {
        throw new RuntimeException(sqlEx);
      }

      throw new RuntimeException(batchEx);
    }
    catch (SQLException ex) {
      logger.error("sql exception: ", ex);
      try {
        store.getConnection().rollback(savepoint);
      }
      catch (SQLException sqlEx) {
        throw new RuntimeException(sqlEx);
      }
      throw new RuntimeException(ex);
    }
    catch (Exception ex) {
      logger.error("exception: ", ex);
      DTThrowable.rethrow(ex);
    }
    finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
      }
      catch (SQLException ex) {
        logger.error("sql exception: ", ex);
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Check to see if the first tuple of th batch is already inserted in the database
   * @param batch
   */
  @Override
  protected boolean ensureBatchNotExecuted(Batch batch)
  {
    try {
      TableMeta tableMeta = tables.get(batch.tableName);
      PreparedStatement stmt = store.getConnection().prepareStatement(tableMeta.countSql);
      String[] row = batch.rows.get(0);
        for (int i = 0; i < row.length; i++) {
          stmt.setObject(i + 1, row[i]);
        }

      ResultSet resultSet = stmt.executeQuery();
      return resultSet.first();
    }
    catch (SQLException ex) {
      DTThrowable.rethrow(ex);
    }

    return false;
  }

  protected List<String[]> fetchRows(FileMeta fileMeta)
  {
    List<String[]> rows;

    int retryNum = fsRetryAttempts;

    while (true) {
      try {
        rows = attempFetchRows(fileMeta);
        return rows;
      }
      catch (IOException ioEx) {
        // wait for some time and retry again as upstream operator might be recovering the file in case of failure failure
        logger.error("exception while reading file {} will retry in {} ms \n exception: ", fileMeta.fileName, fsRetryWaitMillis, ioEx);
        if (retryNum-- > 0) {
          try {
            Thread.sleep(fsRetryWaitMillis);
          }
          catch (InterruptedException ex1) {
            DTThrowable.rethrow(ex1);
          }
        }
        else {
          logger.error("no more retries, giving up reading the file! {}: ", fileMeta);
          return null;
        }
      }
      catch (Exception ex) {
        logger.error("exception: ", ex);
      }
    }
  }

  private List<String[]> attempFetchRows(FileMeta fileMeta) throws IOException
  {
    BufferedReader bufferedReader = null;
    try {
      Path path = new Path(filePath + File.separator + fileMeta.fileName);
      logger.debug("path = {}", path);
      FSDataInputStream inputStream = fs.open(path);
      inputStream.seek(fileMeta.offset);

      bufferedReader = new BufferedReader(new InputStreamReader(inputStream), 1024 * 1024);

      List<String[]> rows = Lists.newArrayList();
      logger.debug("reading {} rows from file {}", fileMeta.numLines, path);
      for (int lineNum = 0; lineNum < fileMeta.numLines; lineNum++) {
        String line = bufferedReader.readLine();
        String[] row = line.split(delimiter);
        rows.add(row);
      }

      logger.debug("all rows added...row list size = {}", rows.size());
      return rows;
    }
    finally {
      if (bufferedReader != null) {
        bufferedReader.close();
      }
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    if (fs != null) {
      try {
        fs.close();
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public JdbcStore getStore()
  {
    return store;
  }

  public void setStore(JdbcStore store)
  {
    this.store = store;
  }

  public String getFilePath()
  {
    return filePath;
  }

  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  public long getFsRetryWaitMillis()
  {
    return fsRetryWaitMillis;
  }

  public void setFsRetryWaitMillis(long fsRetryWaitMillis)
  {
    this.fsRetryWaitMillis = fsRetryWaitMillis;
  }

  public int getFsRetryAttempts()
  {
    return fsRetryAttempts;
  }

  public void setFsRetryAttempts(int fsRetryAttempts)
  {
    this.fsRetryAttempts = fsRetryAttempts;
  }

  public String getDelimiter()
  {
    return delimiter;
  }

  public void setDelimiter(String delimiter)
  {
    this.delimiter = delimiter;
  }

  public static class TableMeta {
    String tableName;
    String insertSql;
    String countSql;
  }

  private static final Logger logger = LoggerFactory.getLogger(JdbcBatchInsertOperator.class);
}

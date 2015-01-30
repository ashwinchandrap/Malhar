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

import javax.validation.constraints.NotNull;

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
import com.datatorrent.contrib.vertica.JdbcBatchInsertOperator.InsertBatch;

/**
 *
 * @param <FILEMETA>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class JdbcBatchInsertOperator<FILEMETA extends FileMeta> extends AbstractNonBlockingExternalWriter<FILEMETA, FILEMETA, InsertBatch>
{
  private String insertSql;
  protected transient FileSystem fs;
  @NotNull
  protected String filePath;
  private transient long fsRetryWaitMillis = 1000L;
  private transient int fsRetryAttempts = 3;
  private String delimiter = "\\|";
  private JdbcStore store = new JdbcStore();

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

  }

  @Override
  protected void processTuple(FILEMETA input)
  {
    enqueueForProcessing(input);
  }

  @Override
  protected InsertBatch generateBatch(FileMeta queueTuple)
  {
    InsertBatch batch = new InsertBatch();
    batch.rows = getRows(queueTuple);
    batch.insertSql = getInsertSql(queueTuple);

    return batch;
  }

  @Override
  protected void executeBatch(InsertBatch batch)
  {
    PreparedStatement stmt = null;
    Savepoint savepoint = null;
    try {
      logger.debug("insert sql = {}", batch.insertSql);
      savepoint = store.getConnection().setSavepoint();
      stmt = store.getConnection().prepareStatement(batch.insertSql);

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

  protected List<String[]> getRows(FileMeta fileMeta)
  {
    List<String[]> rows;

    int retryNum = fsRetryAttempts;

    while (true) {
      try {
        rows = getRowsRetry(fileMeta);
        return rows;
      }
      catch (IOException ioEx) {
        // wait for some time and retry again as upstream operator might be recovering the file after its failure
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
          logger.error("no more retries, giving up reading the file!, exception: ", ioEx);
          DTThrowable.rethrow(ioEx);
        }
      }
      catch (Exception ex) {
        logger.error("exception: ", ex);
      }
    }
  }

  private List<String[]> getRowsRetry(FileMeta fileMeta) throws IOException
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

  protected String getInsertSql(FileMeta input)
  {
    // default implementation: can overtide for improved implementation to write using custom insertSql based on tuple
    return insertSql;
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

  public static class InsertBatch
  {
    String insertSql;
    List<String[]> rows;
  }

  private static final Logger logger = LoggerFactory.getLogger(JdbcBatchInsertOperator.class);
}

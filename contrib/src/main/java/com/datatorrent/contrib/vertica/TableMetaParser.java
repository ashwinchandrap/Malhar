/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.vertica;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.contrib.vertica.JdbcBatchInsertOperator.TableMeta;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public abstract class TableMetaParser
{
  public abstract Map<String, TableMeta> extractTableMeta(String fileName);

  public String getCountQuery(String table, String[] columns)
  {
    return JdbcUtil.buildCountSql(table, columns);
  }

  public String getInsertQuery(String table, String[] columns)
  {
    return JdbcUtil.buildInsertSql(table, columns);
  }

  protected Properties loadFunctionsFromProperties(String fileName)
  {
    InputStream in = null;
    Properties properties;
    try {
      properties = new Properties();
      in = getClass().getResourceAsStream(fileName);
      properties.load(in);
      return properties;
    }
    catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }
    finally {
      if (in != null) {
        try {
          in.close();
        }
        catch (IOException ex) {
          DTThrowable.rethrow(ex);
        }
      }
    }

    return null;
  }

  public static class TablePropertyFileParser extends TableMetaParser
  {
    @Override
    public Map<String, TableMeta> extractTableMeta(String fileName)
    {
      Properties properties = loadFunctionsFromProperties(fileName);

      Map<String, TableMeta> tables = Maps.newHashMap();
      for (Entry<Object, Object> entry : properties.entrySet()) {
        String key = (String)entry.getKey();
        String value = (String)entry.getValue();

        tables.put(key, getTable(key, value));
      }

      return tables;
    }


    private TableMeta getTable(String key, String value)
    {
      TableMeta table = new TableMeta();
      table.tableName = key;
      String[] columns = value.split(":");

      table.countSql = getCountQuery(key, columns);
      table.insertSql = getInsertQuery(key, columns);

      return table;
    }

  }

}

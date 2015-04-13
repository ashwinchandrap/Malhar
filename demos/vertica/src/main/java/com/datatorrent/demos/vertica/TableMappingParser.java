/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.vertica;

import com.datatorrent.contrib.vertica.JdbcBatchInsertOperator.TableMeta;
import com.datatorrent.contrib.vertica.JdbcUtil;
import com.datatorrent.contrib.vertica.TableMetaParser;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class TableMappingParser extends TableMetaParser
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
    TableMeta tableMeta = new TableMeta();
    tableMeta.tableName = key;

    logger.debug("tablename = {}", key);
    String[] colMetas = value.split(",");
    String[] columns = new String[colMetas.length];
    int[] types = new int[colMetas.length];

    for (int i = 0; i < colMetas.length; i++) {
      String colMeta = colMetas[i];
      String[] colMetaArr = colMeta.split(":");
      logger.debug("column meta = {}", Arrays.asList(colMetaArr));
      columns[i] = colMetaArr[0];
      types[i] = JdbcUtil.getType(colMetaArr[1]);
      // handle index 1+ later
    }

    tableMeta.columns = columns;
    tableMeta.types = types;
    tableMeta.countSql = getCountQuery(key, columns);
    tableMeta.insertSql = getInsertQuery(key, columns);

    return tableMeta;
  }

  private static final Logger logger = LoggerFactory.getLogger(TableMappingParser.class);
}

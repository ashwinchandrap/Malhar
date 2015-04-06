/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.vertica;

import com.datatorrent.common.util.Pair;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class JdbcSqlBuilder
{
  public static String buildSelectSql(String tableName, String[] selectColumns, String[] whereColumns)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public static String buildSelectSql(String tableName, String[] selectColumns, String[] whereColumns, Pair<String, String>[] orderByColumns, int limit)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public static String buildInsertSql(String tableName, String[] columns)
  {
    StringBuilder sb = new StringBuilder(100);
    sb.setLength(0);
    StringBuilder valuePlaceHolders = new StringBuilder(100);

    for (String column : columns) {
      if (sb.length() == 0) {
        sb.append(column);
        valuePlaceHolders.append("?");
      }
      else {
        sb.append(",").append(column);
        valuePlaceHolders.append(",?");
      }
    }

    StringBuilder sql = new StringBuilder("INSERT INTO ");
    sql.append(tableName);
    sql.append(" (").append(sb).append(") VALUES");
    sql.append(" (").append(valuePlaceHolders).append(")");

    return sql.toString();
  }

  public static String buildCountSql(String tableName, String[] columns)
  {
    StringBuilder sb = new StringBuilder(100);
    sb.setLength(0);

    StringBuilder sql = new StringBuilder("SELECT COUNT (*) FROM ");

    sql.append(tableName).append(" WHERE ");

    for (String column : columns) {
      if (sb.length() == 0) {
        sb.append(column).append("=?");
      }
      else {
        sb.append("AND ").append(column).append("=?");
      }
    }

    sql.append(sb);
    return sql.toString();
  }

}

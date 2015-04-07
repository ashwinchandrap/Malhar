/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.vertica;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class Batch
{
  public String tableName;
  public int tableId;
  public List<String[]> rows;

  @Override
  public String toString()
  {
    return "Batch{" + "table=" + tableName + ", tableId=" + tableId + ", rows=" + rows + '}';
  }

  private String getRowsAsString()
  {
    if (rows == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder(100);

    builder.append("{");
    for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
      if (rowIdx != 0) {
        builder.append(", ");
      }

      builder.append("Row ").append(rowIdx).append(":[");

      String[] row = rows.get(rowIdx);
      for (int colIdx = 0; colIdx < row.length; colIdx++) {
        if (colIdx != 0) {
          builder.append(", ");
        }
        builder.append(row[colIdx]);
      }
      builder.append("]");
    }
    builder.append("}");
    return builder.toString();
  }

  public List<String> getTokenSeparatedRowList(String token)
  {
    List<String> rowList = new ArrayList<String>();

    StringBuilder rowStr = new StringBuilder(100);
    for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
      String[] row = rows.get(rowIdx);
      for (int colIdx = 0; colIdx < row.length; colIdx++) {
        if (colIdx != 0) {
          rowStr.append(token);
        }
        rowStr.append(row[colIdx]);
      }
      rowList.add(rowStr.toString());
      rowStr.setLength(0);
    }
    return rowList;
  }

  @Override
  public int hashCode()
  {
    int hash = 3;
    hash = 83 * hash + (this.tableName != null ? this.tableName.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Batch other = (Batch)obj;
    if ((this.tableName == null) ? (other.tableName != null) : !this.tableName.equals(other.tableName)) {
      return false;
    }
    return true;
  }

}

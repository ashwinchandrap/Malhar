/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.vertica;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class FileMeta
{
  public String fileName;
  public long offset;
  public long length;
  public int numLines;
  public String tableName;
  public int tableId;

  @Override
  public String toString()
  {
    return "FileMeta{" + "fileName=" + fileName + ", offset=" + offset + ", length=" + length + ", numLines=" + numLines + ", tableName=" + tableName + ", tableId=" + tableId + '}';
  }
}

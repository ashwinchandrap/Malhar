/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io.fs;

import com.datatorrent.lib.io.fs.HdfsFileDataOutputOperator.FileData;
import com.datatorrent.lib.io.fs.HdfsFileDataOutputOperator.FileData.FileInfo;
import java.util.Date;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class HdfsFileDataOutputOperator extends AbstractHdfsTupleFileOutputOperator<FileData, FileData.FileInfo>
{
  @Override
  protected String getFileName(FileData t)
  {
    return t.info.name;
  }

  @Override
  protected FileInfo getOutputTuple(FileData t)
  {
    return t.info;
  }

  @Override
  protected byte[] getBytesForTuple(FileData t)
  {
    return t.data.getBytes();
  }

  public static class FileData
  {
    public static class FileInfo
    {
      public String name;
      public Date date;
    }

    public FileInfo info = new FileInfo();
    public String data;
  }

  private static final long serialVersionUID = 201405151847L;
}

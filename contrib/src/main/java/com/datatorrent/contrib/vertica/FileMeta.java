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
  String fileName;
  long offset;
  long length;
  int numLines;
}

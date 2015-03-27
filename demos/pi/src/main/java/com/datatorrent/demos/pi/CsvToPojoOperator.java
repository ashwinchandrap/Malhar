/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import java.io.IOException;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.io.ICsvReader;
import org.supercsv.prefs.CsvPreference;

import com.datatorrent.lib.util.ReusableStringReader;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.contrib.parser.AbstractCsvParser;
import java.io.StringReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class CsvToPojoOperator extends AbstractCsvParser<Object>
{
  ICsvBeanReader csvBeanReader;
  Class<?> beanClass;
  ReusableStringReader reader;

  @Override
  protected ICsvReader getReader(ReusableStringReader reader, CsvPreference preference)
  {
    this.reader = reader;
    csvBeanReader = new CsvBeanReader(reader, preference);
    return csvBeanReader;
  }

  @Override
  protected Object readData(String[] properties, CellProcessor[] processors)
  {
    try {
      return csvBeanReader.read(beanClass, properties, processors);
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    return null;
  }

  private static final Logger logger = LoggerFactory.getLogger(CsvToPojoOperator.class);
}

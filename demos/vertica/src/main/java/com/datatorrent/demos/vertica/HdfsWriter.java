/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.vertica;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.fs.FSDataOutputStream;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.contrib.vertica.Batch;
import com.datatorrent.contrib.vertica.FileMeta;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class HdfsWriter extends AbstractFileOutputOperator<Batch>
{
  private transient Map<String, Long> lastOffsets;
  private transient Map<String, Integer> tableIds = Maps.newHashMap();
  private Map<String, MutableInt> numLinesPerFile = Maps.newHashMap();
  private transient StringBuilder sb = new StringBuilder(100);
  //private TreeMap<Long, List<FeedFileMeta>> windowFileOffsets = Maps.newTreeMap();
  private long currentWindowId;
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<FileMeta> offsetOutput = new DefaultOutputPort<FileMeta>();
  private String applicationId = null;
  private transient int numLinesInTuple = 0;
  private final int batchWindowsNum = 1;
  private int windowCount = 0;

  public HdfsWriter()
  {
    streamCodec = new HdfsOutputStreamCodec();
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    lastOffsets = Maps.newHashMap();
    loadLastOffsets();
    logger.debug("lastoffsets = {}", lastOffsets);
    if (applicationId == null) {
      applicationId = context.getValue(DAG.APPLICATION_ID);
      filePath = filePath + "/" + applicationId;
    }
  }

  @Override
  protected String getFileName(Batch tuple)
  {
    return tuple.tableName;
  }

  @Override
  protected byte[] getBytesForTuple(Batch tuple)
  {
    List<String> rowList = tuple.getTokenSeparatedRowList("|");

    sb.setLength(0);

    for (String row : rowList) {
      sb.append(row).append("\n");
    }

    numLinesInTuple = rowList.size();
    return sb.toString().getBytes();
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;

    loadLastOffsets();
    logger.debug("lastoffsets = {}", lastOffsets);
  }

  private void loadLastOffsets()
  {
    lastOffsets = Maps.newHashMap();

    for (Entry<String, MutableLong> entry : endOffsets.entrySet()) {
      String file = entry.getKey();
      MutableLong offset = entry.getValue();

      lastOffsets.put(file, offset.toLong());
    }
  }

  @Override
  protected void processTuple(Batch tuple)
  {
    if (!tableIds.containsKey(tuple.tableName)) {
      tableIds.put(tuple.tableName, tuple.tableId);
    }
    String fileName = getFileName(tuple);

    if (Strings.isNullOrEmpty(fileName)) {
      return;
    }

    try {
      FSDataOutputStream fsOutput = streamsCache.get(fileName);
      byte[] tupleBytes = getBytesForTuple(tuple);
      fsOutput.write(tupleBytes);
      totalBytesWritten += tupleBytes.length;
      MutableLong currentOffset = endOffsets.get(fileName);

      if (currentOffset == null) {
        currentOffset = new MutableLong(0);
        endOffsets.put(fileName, currentOffset);
      }

      if (lastOffsets.get(fileName) == null) {
        lastOffsets.put(fileName, currentOffset.toLong());
      }
      currentOffset.add(tupleBytes.length);

      MutableInt numLines = numLinesPerFile.get(fileName);
      if (numLines == null) {
        numLines = new MutableInt(0);
        numLinesPerFile.put(fileName, numLines);
      }
      numLines.add(numLinesInTuple);

      if (rollingFile && currentOffset.longValue() > maxLength) {
        emitOffset(fileName, currentOffset.longValue());
        rotate(fileName);
      }

      MutableLong count = counts.get(fileName);
      if (count == null) {
        count = new MutableLong(0);
        counts.put(fileName, count);
      }

      count.add(1);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    catch (ExecutionException ex) {
      throw new RuntimeException(ex);
    }

  }

  @Override
  public void endWindow()
  {
    windowCount++;
    super.endWindow();

    if (windowCount >= batchWindowsNum) {
      for (Entry<String, MutableLong> entry : endOffsets.entrySet()) {
        emitOffset(entry.getKey(), entry.getValue().longValue());
      }
      windowCount = 0;
    }
  }

  private void emitOffset(String fileName, long offset)
  {
    long beginOffset = lastOffsets.get(fileName);
    long length = offset - beginOffset;

    logger.debug("filename = {}, offset = {}", fileName, offset);
    logger.debug("window = {} length = {}", currentWindowId, length);

    if (length == 0L) {
      return;
    }

    FileMeta flushedOffset = new FileMeta();
    flushedOffset.tableName = fileName;
    flushedOffset.fileName = getPartFileName(fileName, openPart.get(fileName).intValue());
    flushedOffset.offset = beginOffset;
    flushedOffset.length = length;
    flushedOffset.numLines = numLinesPerFile.get(fileName).intValue();
    flushedOffset.tableId = tableIds.get(flushedOffset.tableName);

    numLinesPerFile.remove(fileName);

    offsetOutput.emit(flushedOffset);
    lastOffsets.remove(fileName);
  }



  private static final Logger logger = LoggerFactory.getLogger(HdfsWriter.class);
}

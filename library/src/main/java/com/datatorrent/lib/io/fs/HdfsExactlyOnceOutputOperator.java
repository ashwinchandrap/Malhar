/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.lib.io.fs;

import java.io.IOException;
import java.io.Serializable;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.Stats.OperatorStats.CustomStats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * HDFSOutput Operator that writes the data exactly once.
 * The Operator creates file <window_id>.tmp in the beginwindow and writes the tuples to it.
 * It moves the file to <window_id> in the end window.
 * If the operator fails and recover, checks if the file <window_id> exists in begin window. If it does,
 * then the operator doesn't process anything during that window. If it doesn't, then the operator deletes
 * the <window_id>.tmp file if it exists and creates new and starts writing to it.
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class HdfsExactlyOnceOutputOperator extends AbstractHdfsFileOutputOperator<String>
{
  private final String TEMP = ".tmp";
  private transient Path currentFilePath;
  private transient Path currentTempFilePath;
  private transient LogCounter logCounter;
  private transient OperatorContext context;
  @NotNull
  private String logType;

  @Override
  protected void processTuple(String t)
  {
    try {
      // if stream is not open, then do nothing since the file already exists for current window
      if (fsOutput == null) {
        return;
      }

      byte[] tupleBytes = getBytesForTuple(t);

      if (bufferedOutput != null) {
        bufferedOutput.write(tupleBytes);
      }
      else {
        fsOutput.write(tupleBytes);
      }
      totalBytesWritten += tupleBytes.length;
      logCounter.count++;

    }
    catch (IOException ex) {
      throw new RuntimeException("Failed to write to stream.", ex);
    }

  }

  @Override
  public void beginWindow(long windowId)
  {
    logCounter.count = 0;

    try {
      currentFilePath = new Path(filePath + "/" + windowId);
      currentTempFilePath = currentFilePath.suffix(TEMP);
      if (fs.exists(currentFilePath)) {
        fsOutput = null;
      }
      else {
        if (fs.exists(currentTempFilePath)) {
          fs.delete(currentTempFilePath, true);
        }
        openFile(currentTempFilePath);
      }
    }
    catch (IOException e) {
      throw new RuntimeException("Failed to open the file.", e);
    }
  }

  @Override
  public void endWindow()
  {
    context.setCustomStats(logCounter);
    if (fsOutput != null) {
      try {
        closeFile();
        fs.rename(currentTempFilePath, currentFilePath);
      }
      catch (IOException ex) {
        throw new RuntimeException("Failed to flush.", ex);
      }
    }
  }

  @Override
  protected byte[] getBytesForTuple(String t)
  {
    return (t + "\n").getBytes();
  }

  @Override
  public void setAppend(boolean append)
  {
    append = false;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    this.context = context;
    append = false;
    logCounter = new LogCounter();
    logCounter.logType = logType;
  }

  @Override
  public void teardown()
  {
    super.teardown();
    fsOutput = null;
  }

  public String getLogType()
  {
    return logType;
  }

  public void setLogType(String logType)
  {
    this.logType = logType;
  }

  public static class LogCounter implements CustomStats
  {
    private String logType;
    private int count;

    public LogCounter()
    {
    }

    public String getLogType()
    {
      return logType;
    }

    public void setLogType(String logType)
    {
      this.logType = logType;
    }

    public int getCount()
    {
      return count;
    }

    public void setCount(int count)
    {
      this.count = count;
    }

    public void incrementCount()
    {
      count++;
    }

    @Override
    public String toString()
    {
      return "LogCounter{" + "logType=" + logType + ", count=" + count + '}';
    }

  }

  /**
   * Added just for debugging for now, can be used to aggregate the counters at the logical operator level
   */
  public static class LogsCounterListener implements StatsListener, Serializable
  {
    @Override
    public Response processStats(BatchedOperatorStats bos)
    {
      LogCounter counter;
      for (OperatorStats os : bos.getLastWindowedStats()) {
        counter = ((LogCounter)os.customStats);
        counterLogger.debug("counter = {}", counter);
      }
      return null;
    }

    private static final Logger counterLogger = LoggerFactory.getLogger(LogsCounterListener.class);
  }

  private static final long serialVersionUID = 201405201214L;
  private static final Logger logger = LoggerFactory.getLogger(HdfsOutputOperator.class);
}

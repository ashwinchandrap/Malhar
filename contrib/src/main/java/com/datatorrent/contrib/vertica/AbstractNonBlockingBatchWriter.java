/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.vertica;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Queues;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.io.fs.AbstractReconciler;

import com.datatorrent.api.Context.OperatorContext;

import com.datatorrent.common.util.NameableThreadFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

/**
 * This is used when writing batches to the external system is slower than the generation of the batch
 * @param <INPUT>
 * @param <QUEUETUPLE>
 * @param <BATCH>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public abstract class AbstractNonBlockingBatchWriter<INPUT, QUEUETUPLE, BATCH> extends AbstractReconciler<INPUT, QUEUETUPLE>
{
  private transient ExecutorService batchExecute;
  // upto three batches will be available for external writer
  private BlockingQueue<BATCH> batchQueue = Queues.newLinkedBlockingQueue(3);
  protected Map<String, BATCH> partialBatches = Maps.newHashMap(); // table -- > partial batches, used to cache spillover batches which have less than batchSize numbe of rows;
  protected int batchSize = 10000;
  protected int batchFinalizeWindowCount = 10;
  protected boolean tupleInWindow = false;
  protected int noTupleWindowCount = 0;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    batchExecute = Executors.newSingleThreadExecutor(new NameableThreadFactory("BatchExecuteHelper"));
    batchExecute.submit(batchExecuteHandler());

  }

  @Override
  protected final void processTuple(INPUT input)
  {
    if (!tupleInWindow) {
      tupleInWindow = true;
    }

    enqueueForProcessing(convertInputTuple(input));
  }

  /**
   * Batch execute thread used to write batch to external system.
   * This is a separate thread to ensure that external I/O is not blocked by any of the batch load I/O
   *
   * By using blocking queue, it is also ensured that the writer to the queue does not generate more than
   * the set number of batches when the batch writing is slower than batch generation.
   *
   * @return
   */
  private Runnable batchExecuteHandler()
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        try {
          while (execute) {
            while (batchQueue.isEmpty()) {
              Thread.sleep(spinningTime);
            }

            BATCH batch = batchQueue.peek();
            //if(ensureBatchNotExecuted(batch)) {
              executeBatch(batch);
            //}

            batchQueue.remove();
          }
        }
        catch (Throwable e) {
          cause.set(e);
          execute = false;
        }
      }
    };
  }

  @Override
  protected void processCommittedData(QUEUETUPLE queueInput)
  {
    List<BATCH> batches = generateBatches(queueInput);
    try {
      for (BATCH batch : batches) {
        batchQueue.put(batch); // blocks if queue is full
      }
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void endWindow()
  {
    if (tupleInWindow) {
      tupleInWindow = false;
      noTupleWindowCount = 0;
    }
    else {
      noTupleWindowCount++;
    }

    // finalize partial batches
    if (noTupleWindowCount >= batchFinalizeWindowCount && getQueueSize() == 0) {
      List<BATCH> batches = retreivePartialBatches();
      try {
        for (BATCH batch : batches) {
          batchQueue.put(batch); // blocks if queue is full
        }
      }
      catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }

      noTupleWindowCount = 0;
    }
  }

  protected List<BATCH> retreivePartialBatches()
  {
    List<BATCH> batches = Lists.newArrayList();
    for (Entry<String, BATCH> entry : partialBatches.entrySet()) {
      BATCH batch = entry.getValue();
      batches.add(batch);
    }

    return batches;
  }

  protected abstract QUEUETUPLE convertInputTuple(INPUT input);

  protected abstract List<BATCH> generateBatches(QUEUETUPLE queueTuple);

  protected abstract void executeBatch(BATCH batch);

  /**
   * Used to check with external system if the batch has previously been processed or not
   * @param batch
   */
  protected abstract boolean ensureBatchNotExecuted(BATCH batch);

  public int getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public int getBatchFinalizeWindowCount()
  {
    return batchFinalizeWindowCount;
  }

  public void setBatchFinalizeWindowCount(int batchFinalizeWindowCount)
  {
    this.batchFinalizeWindowCount = batchFinalizeWindowCount;
  }

  private static final Logger anbbwLogger = LoggerFactory.getLogger(AbstractNonBlockingBatchWriter.class);
}

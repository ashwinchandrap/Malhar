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

/**
 * This is used when writing batches to the external system is slower than the generation of the batch
 * @param <INPUT>
 * @param <QUEUETUPLE>
 * @param <BATCH>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public abstract class AbstractNonBlockingExternalWriter<INPUT, QUEUETUPLE, BATCH> extends AbstractReconciler<INPUT, QUEUETUPLE>
{
  private ExecutorService batchExecute;
  // upto three batches will be available for external writer
  private ArrayBlockingQueue<BATCH> batchQueue = Queues.newArrayBlockingQueue(3);

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    batchExecute = Executors.newSingleThreadExecutor(new NameableThreadFactory("BatchExecuteHelper"));
    batchExecute.submit(batchExecuteHandler());

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
          while (execute && !batchQueue.isEmpty()) {

            BATCH batch = batchQueue.peek();
            executeBatch(batch);
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
    BATCH batch = generateBatch(queueInput);
    try {
      batchQueue.put(batch); // blocks if queue is full
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  protected abstract BATCH generateBatch(QUEUETUPLE queueTuple);

  protected abstract void executeBatch(BATCH batch);

  private static final Logger logger = LoggerFactory.getLogger(AbstractNonBlockingExternalWriter.class);
}

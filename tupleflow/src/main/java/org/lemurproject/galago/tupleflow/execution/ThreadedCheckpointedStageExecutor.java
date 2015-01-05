// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.lemurproject.galago.tupleflow.ExNihiloSource;
import org.lemurproject.galago.utility.StreamUtil;

/**
 * This executor has no practical use at all. It's only here to make it easy to
 * test the RemoteExecutor base class.
 *
 * This executor is essentially the same as the LocalStageExecutor in that it
 * runs everything in a single thread, but it serializes the job information and
 * then reads it again.
 *
 * @author trevor
 */
public class ThreadedCheckpointedStageExecutor extends CheckpointedStageExecutor {

  ExecutorService threadPool;

  public static class InstanceRunnable implements Runnable {

    StageInstanceDescription description;
    Exception exception;
    boolean isRunning;
    boolean isQueued;
    NetworkedCounterManager counterManager;
    CountDownLatch latch;
    String instanceFile;
    File errorFile;
    File completeFile;

    public InstanceRunnable(String instanceFile,
            NetworkedCounterManager manager,
            CountDownLatch latch) {
      this.isRunning = false;
      this.isQueued = true;
      this.instanceFile = instanceFile;
      this.errorFile = new File(instanceFile + ".error");
      this.completeFile = new File(instanceFile + ".complete");
      this.description = null;
      
      try {
        ObjectInputStream stream = new ObjectInputStream(new FileInputStream(new File(instanceFile)));
        this.description = (StageInstanceDescription) stream.readObject();
      } catch (Exception e) {
        exception = e; // fails from the start.
      }

      this.exception = null;
      this.counterManager = manager;
      this.latch = latch;
    }

    public synchronized Exception getException() {
      return exception;
    }

    public synchronized boolean isQueued() {
      return isQueued;
    }

    public synchronized boolean isRunning() {
      return isRunning;
    }

    public synchronized boolean isDone() {
      return !isQueued && !isRunning;
    }

    private synchronized void setException(Exception e) {
      this.exception = e;
    }

    private synchronized void setIsRunning(boolean isRunning) {
      this.isRunning = isRunning;
    }

    private synchronized void setIsQueued(boolean isQueued) {
      this.isQueued = isQueued;
    }

    @Override
    public void run() {
      try {
        if(description != null){
          setIsQueued(false);
          setIsRunning(true);
          StageInstanceFactory factory = new StageInstanceFactory(counterManager);
          ExNihiloSource source = factory.instantiate(description);
          source.run();
        } 
      } catch (Throwable e) {
        setException(new Exception(e));

      } finally {
        try{
          if(this.exception == null){
            completeFile.createNewFile();
          } else {
            StreamUtil.copyStringToFile(this.exception.toString(), errorFile);
          }
        } catch(IOException e){
          System.err.println(e.toString() + "\nFailed to write complete/error file");
          // don't care about this exception -- could not write complete/error
        }
        
        latch.countDown();
        setIsRunning(false);
      }
    }
  }

  public class ThreadedExecutionStatus implements StageExecutionStatus, Runnable {

    private String stageName;
    private List<String> instanceFiles;
    List<InstanceRunnable> instances = new ArrayList();
    boolean done = false;
    private final NetworkedCounterManager manager;
    private final CountDownLatch latch;

    public ThreadedExecutionStatus(String stageName, List<String> instanceFiles) {
      this.stageName = stageName;
      this.instanceFiles = instanceFiles;

      manager = new NetworkedCounterManager();

      latch = new CountDownLatch(instanceFiles.size());

      try {
        for (String instanceFile : instanceFiles) {
          instances.add(new InstanceRunnable(instanceFile, manager, latch));
        }
      } catch (Exception ex) {
        throw new RuntimeException("Failed to read job file: " + ex.toString());
      }
    }

    @Override
    public void run() {
      synchronized (this) {
        manager.start();
      }

      for (InstanceRunnable instance : instances) {
        threadPool.execute(instance);
      }

      while (latch.getCount() > 0) {
        try {
          // wait 10 seconds
          latch.await(10000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // do nothing
        }
      }

      synchronized (this) {
        manager.stop();
        done = true;
      }

    }

    @Override
    public String getName() {
      return stageName;
    }

    @Override
    public int getBlockedInstances() {
      return 0;
    }

    @Override
    public synchronized int getQueuedInstances() {
      int queuedInstances = 0;

      for (InstanceRunnable instance : instances) {
        if (instance.isQueued()) {
          queuedInstances++;
        }
      }

      return queuedInstances;
    }

    @Override
    public int getRunningInstances() {
      int runningInstances = 0;

      for (InstanceRunnable instance : instances) {
        if (instance.isRunning()) {
          runningInstances++;
        }
      }

      return runningInstances;
    }

    @Override
    public int getCompletedInstances() {
      int completedInstances = 0;

      for (InstanceRunnable instance : instances) {
        if (instance.isDone()) {
          completedInstances++;
        }
      }

      return completedInstances;
    }

    @Override
    public synchronized List<Double> getRunTimes() {
      ArrayList<Double> times = new ArrayList();
      // do something
      return times;
    }

    @Override
    public synchronized List<Exception> getExceptions() {
      ArrayList<Exception> exceptions = new ArrayList();

      for (InstanceRunnable instance : instances) {
        Exception e = instance.getException();
        if (e != null) {
          exceptions.add(e);
        }
      }

      return exceptions;
    }

    @Override
    public boolean isDone() {
      return done;
    }
  }

  public ThreadedCheckpointedStageExecutor() {
    threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  @Override
  public StageExecutionStatus submit(String stageName, ArrayList<String> jobPaths, String temporary) {
    ThreadedExecutionStatus status = new ThreadedExecutionStatus(stageName, jobPaths);
    new Thread(status).start();
    return status;
  }

  @Override
  public void shutdown() {
    threadPool.shutdown();
  }
}

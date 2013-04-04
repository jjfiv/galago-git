// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.tupleflow.ExNihiloSource;

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
public class LocalCheckpointedStageExecutor extends CheckpointedStageExecutor {

  public static class LocalExecutionStatus implements StageExecutionStatus, Runnable {

    private String stageName;
    private List<String> instanceFiles;
    ArrayList<Exception> exceptions = new ArrayList();
    int queuedInstances = 0;
    int runningInstances = 0;
    int completedInstances = 0;
    boolean done = false;

    public LocalExecutionStatus(String stageName, List<String> instanceFiles) {
      this.stageName = stageName;
      this.instanceFiles = instanceFiles;

      queuedInstances = this.instanceFiles.size();
      runningInstances = 0;
      completedInstances = 0;
    }

    @Override
    public void run() {
      List<StageInstanceDescription> instances = new ArrayList();

      for (String instanceFile : instanceFiles) {
        try {
          ObjectInputStream stream = new ObjectInputStream(new FileInputStream(new File(instanceFile)));
          StageInstanceDescription instance = (StageInstanceDescription) stream.readObject();
          instances.add(instance);
        } catch (Exception e) {
          exceptions.add(e);
        }
      }

      if (exceptions.size() > 0) {
        // failed to read an instance file or two.
        done = true;
        return;
      }

      NetworkedCounterManager manager = new NetworkedCounterManager();
      StageInstanceFactory factory = new StageInstanceFactory(manager);
      manager.start();

      for (StageInstanceDescription instance : instances) {
        synchronized (this) {
          runningInstances += 1;
          queuedInstances -= 1;
        }

        try {
          ExNihiloSource source = factory.instantiate(instance);
          source.run();
        } catch (Throwable err) {
          err.printStackTrace();
          addException(new Exception(err));
        }

        synchronized (this) {
          runningInstances -= 1;
          completedInstances += 1;
        }

        if (exceptions.size() > 0) {
          // we have an exception - break and quit cleanly.
          break;
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
      return queuedInstances;
    }

    @Override
    public synchronized int getRunningInstances() {
      return runningInstances;
    }

    @Override
    public int getCompletedInstances() {
      return completedInstances;
    }

    @Override
    public synchronized boolean isDone() {
      return done;
    }

    @Override
    public synchronized List<Exception> getExceptions() {
      return exceptions;
    }

    @Override
    public synchronized List<Double> getRunTimes() {
      ArrayList<Double> times = new ArrayList();
      // do something
      return times;
    }

    private synchronized void addException(Exception e) {
      exceptions.add(e);
    }
  }

  @Override
  public StageExecutionStatus submit(String stageName, ArrayList<String> jobPaths, String temporary) {
    LocalExecutionStatus status = new LocalExecutionStatus(stageName, jobPaths);
    new Thread(status).start();
    return status;
  }

  @Override
  public void shutdown() {
  }
}

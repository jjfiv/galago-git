// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This will eventually allow you to run TupleFlow workers on other machines using
 * SSH.  For now, it doesn't work.  If you really need job distribution, try the Grid
 * Engine plugin.
 * 
 * @author trevor
 */
public class SSHStageExecutor extends RemoteStageExecutor {

  private static String machineEndMarker = "STOP#MACHINE#SHUTDOWN";
  private LinkedBlockingQueue<String> machines = new LinkedBlockingQueue();
  private LinkedBlockingQueue<StageTask> tasks = new LinkedBlockingQueue();
  private ExecutorService pool = null;
  private String commandName;

  public SSHStageExecutor(String commandName, List<String> machines) {
    this.commandName = commandName;
    this.machines.addAll(machines);
    this.pool = Executors.newCachedThreadPool();
  }

  public class SSHStageExecutionContext implements Runnable, StageExecutionStatus {

    String name;
    List<String> jobPaths;
    boolean done;

    public SSHStageExecutionContext(String name, List<String> jobPaths) {
      this.name = name;
      this.jobPaths = jobPaths;
      this.done = false;
    }

    public void run() {
      CountDownLatch latch = new CountDownLatch(jobPaths.size());

      for (String jobPath : jobPaths) {
        // submit this job to the queue
        StageTask task = new StageTask(commandName, jobPath, latch);
        pool.execute(task);
      }

      synchronized (this) {
        done = true;
      }
    }

    public synchronized boolean isDone() {
      return done;
    }

    public String getName() {
      return name;
    }

    public int getBlockedInstances() {
      return 0;
    }

    public int getQueuedInstances() {
      // FIXME
      return jobPaths.size();
    }

    public int getRunningInstances() {
      // FIXME
      return 0;
    }

    public int getCompletedInstances() {
      // FIXME
      return 0;
    }

    public synchronized List<Double> getRunTimes() {
      ArrayList<Double> times = new ArrayList();
      // do something
      return times;
    }

    public List<Exception> getExceptions() {
      // FIXME
      return Collections.EMPTY_LIST;
    }
  }

  public class StageTask implements Runnable {

    private String commandName;
    private String jobFileArgument;
    private CountDownLatch latch;

    public StageTask(String commandName, String jobFileArgument, CountDownLatch latch) {
      this.commandName = commandName;
      this.jobFileArgument = jobFileArgument;
      this.latch = latch;
    }

    public void run() {
      String machineName;

      try {
        // first, wait for a machine reservation
        machineName = machines.poll();

        // if we see a shutdown marker, quit, but put the marker back
        if (machineName.equals(machineEndMarker)) {
          machines.offer(machineEndMarker);
          return;
        }

        String[] arguments = new String[]{machineName, jobFileArgument};
        Process process = Runtime.getRuntime().exec(commandName, arguments);

        // close the process stdin
        process.getOutputStream().close();
        // BUGBUG: someday we need to trap process stdout/stderr here

        process.waitFor();
      } catch (Exception e) {
        // BUGBUG: fix this too
        e.printStackTrace();
      } finally {
        latch.countDown();
      }
    }

    public boolean isNullTask() {
      return commandName == null && jobFileArgument == null && latch == null;
    }
  }

  public StageExecutionStatus submit(String stageName, ArrayList<String> jobPaths, String temporary) {
    SSHStageExecutionContext context = new SSHStageExecutionContext(stageName, jobPaths);
    new Thread(context).start();
    return context;
  }

  public void shutdown() {
    machines.add(machineEndMarker);
    pool.shutdown();
  }
}

package org.lemurproject.galago.tupleflow.forkmode;

import org.lemurproject.galago.tupleflow.execution.StageExecutionStatus;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author jfoley
 */
public class ForkModeResult implements StageExecutionStatus {

  ArrayList<ForkModeProcessRunner> jobs;
  HashMap<String, File> jobCheckpoints = null;
  HashMap<String, Long> startTimes = null;
  HashMap<String, Long> stopTimes = null;
  ArrayList<Exception> exceptions;
  String stageName;

  public ForkModeResult(String n, ArrayList<ForkModeProcessRunner> jobs, HashMap<String, File> jobCheckpoints, HashMap<String, Long> starts) {
    this(n, jobs, jobCheckpoints, starts, null);
  }

  public ForkModeResult(String n, ArrayList<ForkModeProcessRunner> jobs, HashMap<String, File> jobCheckpoints, HashMap<String, Long> starts, Exception e) {
    this.jobs = jobs;
    this.jobCheckpoints = jobCheckpoints;
    this.startTimes = starts;
    this.stopTimes = new HashMap<>();
    this.stageName = n;
    this.exceptions = new ArrayList<>();
    if (e != null) {
      this.exceptions.add(e);
    }
  }

  @Override
  public ArrayList<Exception> getExceptions() {

    for (ForkModeProcessRunner job : jobs) {
      if (job.isStillRunning()) continue;

      int status = job.getReturnCode();

      if (status != 0) {
        exceptions.add(new Exception("[" + job + "] failed -- see stderr folder."));
        System.err.println("[" + job + "] failed -- see stderr folder.");

      } else {

        // check for X.complete
        File checkpoint = jobCheckpoints.get(job.getName());
        boolean exists = false;
        int count = 0;
        do {
          if (checkpoint.exists()) {
            exists = true;
            break;
          }
          try {
            Thread.sleep(1000); // wait 1 second
          } catch (InterruptedException ignored) {
            // Don't care about interruption errors
          }
        } while (count++ < 60); // 1 min timeout

        // if the job finished over a minute ago, and the checkpoint still doesn't exist - it probably errored.
        if (!exists) {
          // add an exception.
          exceptions.add(new Exception("[" + job + "] failed -- checkpoint does not exist."));
          System.err.println("[" + job + "] failed -- checkpoint does not exist.");
        }

      }
    }

    return exceptions;
  }

  @Override
  public String getName() {
    return stageName;
  }

  @Override
  public int getCompletedInstances() {
    int comp = 0;
    for (ForkModeProcessRunner job : jobs) {
      if (!job.isStillRunning()) {
        comp += 1;
      }
    }
    return comp;
  }

  @Override
  public int getRunningInstances() {
    int running = 0;
    for (ForkModeProcessRunner job : jobs) {
      if (job.isStillRunning()) {
        running += 1;
      }
    }
    return running;
  }

  @Override
  public int getQueuedInstances() {
    return 0;
  }

  @Override
  public int getBlockedInstances() {
    return 0;
  }

  @Override
  public synchronized List<Double> getRunTimes() {
    long now = System.currentTimeMillis();
    ArrayList<Double> times = new ArrayList<>();
    if (startTimes != null) {
      for (ForkModeProcessRunner job : jobs) {
        long start = startTimes.get(job.getName());
        long end = job.getStopTime();
        if (end == 0) {
          end = now;
        }
        double diff = (end - start) / 1000.0;
        times.add(diff);
      }
    }
    return times;
  }

  @Override
  public boolean isDone() {
    return getCompletedInstances() == jobs.size();
  }
}

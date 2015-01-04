//BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SessionFactory;
import org.lemurproject.galago.tupleflow.GalagoConf;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DRMAAExecutor
 *
 * March 30, 2007 -- Trevor Strohman
 * March 31, 2010 -- Sam Huston, Stanford Chiu
 * Sept 16,  2010 -- irmarc
 *    - Added code for getting timing results
 *
 * Usage : StageExecutionFactory
 *  - when a user submits a job with mode="drmaa"
 *
 * - Keeps track of all stage instances submitted to the drmaa api
 * - Provides some statistics on the running stages
 *
 * @author trevor, sjh, schiu, irmarc
 */
public class DRMAAStageExecutor extends CheckpointedStageExecutor {

  private static final Logger logger = Logger.getLogger(DRMAAStageExecutor.class.getName());

  Session session;
  // Flag to set the verbose mode (either on or off)
  public boolean verbose;
  // For use with user-defined native specifications
  public String nativeSpecification_each;
  public String nativeSpecification_combined;
  // Use the 'java' specified in the env. variable JAVA_HOME --
  // we don't know what version of java the user called us with :(
  public String command = System.getenv("JAVA_HOME") + File.separator
          + "bin/java";
  // We do know what class path the user envoked us with, so use that
  // when submitting each of the jobs to the cluster.
  public String classPath = System.getProperty("java.class.path");
  // This is the TupleFlow executor we'll be using for each of the jobs.
  public String className = org.lemurproject.galago.tupleflow.execution.LocalStageExecutor.class.getCanonicalName();
  // Arbitrary starting and max heap sizes.
  public static final String MEMORY_X = "-Xmx1700m";
  public static final String MEMORY_S = "-Xms1700m";
  public static final String DEFAULT_ENCODING = System.getProperty("file.encoding", "UTF-8");
  // This holds the location that should be used to write temporary files
  // on the nodes.
  public static final String NODE_TEMP_DIR =
          System.getProperty("java.io.tmpdir");
  // These will hold the starting and max heaps passed in by the executor
  // calling us, if any.
  public String memory_x;
  public String memory_s;
  public String nodeTempDir;

  public class DRMAAResult implements StageExecutionStatus {

    ArrayList<String> jobs;
    HashMap<String, File> jobCheckpoints = null;
    HashMap<String, Long> startTimes = null;
    HashMap<String, Long> stopTimes = null;
    ArrayList<Exception> exceptions;
    String stageName;

    public DRMAAResult(String n, ArrayList<String> jobs, HashMap<String, File> jobCheckpoints, HashMap<String, Long> starts) {
      this(n, jobs, jobCheckpoints, starts, null);
    }

    public DRMAAResult(String n, ArrayList<String> jobs, HashMap<String, File> jobCheckpoints, HashMap<String, Long> starts, Exception e) {
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

      for (String job : jobs) {
        try {
          int status = session.getJobProgramStatus(job);

          if (status == Session.FAILED) {
            exceptions.add(new Exception("[" + job + "] failed -- see stderr folder."));
            System.err.println("[" + job + "] failed -- see stderr folder.");

          } else if (status == Session.DONE) {

            // check for X.complete
            File checkpoint = jobCheckpoints.get(job);
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
        } catch (DrmaaException e) {
          System.err.println("Could not error check the drmaa session!");
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
      try {
        for (String job : jobs) {
          int status = session.getJobProgramStatus(job);

          //if( status == session.FAILED )
          //System.err.println( "[" + job + "] failed." );

          if (status == Session.DONE || status == Session.FAILED) {
            comp += 1;
          }
        }
      } catch (DrmaaException e) {
        return 0;
      }

      return comp;
    }

    @Override
    public int getRunningInstances() {
      int comp = 0;
      try {
        for (String job : jobs) {
          int status = session.getJobProgramStatus(job);

          comp += 1;

          //if( status == session.FAILED )
          //System.err.println( "[" + job + "] failed." );

          if (status == Session.QUEUED_ACTIVE || status == Session.DONE || status == Session.FAILED) {
            comp -= 1;
          }
        }
      } catch (DrmaaException e) {
        return 0;
      }

      return comp;
    }

    @Override
    public int getQueuedInstances() {
      int queued = 0;
      try {
        for (String job : jobs) {
          int status = session.getJobProgramStatus(job);

          if (status == Session.QUEUED_ACTIVE) {
            queued += 1;
          }
        }
      } catch (DrmaaException e) {
        return 0;
      }

      return queued;
    }

    @Override
    public int getBlockedInstances() {
      return 0;
    }

    @Override
    public synchronized List<Double> getRunTimes() {
      ArrayList<Double> times = new ArrayList<>();
      long current;
      if (startTimes != null) {
        for (String jobid : startTimes.keySet()) {
          try {
            int status = session.getJobProgramStatus(jobid);
            long start = startTimes.get(jobid);
            if (status == Session.DONE || status == Session.FAILED) {
              if (!stopTimes.containsKey(jobid)) {
                stopTimes.put(jobid, System.currentTimeMillis());
              }
              current = stopTimes.get(jobid);
            } else {
              current = System.currentTimeMillis();
            }
            double diff = (current - start + 0.0) / 1000.0;
            times.add(diff);
          } catch (DrmaaException e) {
            logger.log(Level.WARNING, e.getMessage(), e);
          }
        }
      }
      return times;
    }

    @Override
    public boolean isDone() {
      try {
        for (String job : jobs) {
          int status = session.getJobProgramStatus(job);

          //if( status == session.FAILED )
          //System.err.println( "[" + job + "] failed." );

          if (status == Session.DONE || status == Session.FAILED) {
            continue;
          }

          return false;
        }
      } catch (DrmaaException e) {
        return false;
      }

      return true;
    }
  }

  /**
   * <p>Creates a new create of DRMAAExecutor.</p>
   *
   * @param args     An array; if it contains anything, the first
   *                 element is used as the command when submitting
   *                 jobs to DRMAA.
   */
  public DRMAAStageExecutor(String[] args) {

    // Set the defaults for each job.
    setMemoryUsage(MEMORY_X, MEMORY_S);
    nodeTempDir = NODE_TEMP_DIR;
    verbose = false;
    nativeSpecification_each = "-w n";
    nativeSpecification_combined = "-w n";

    Parameters defaults = GalagoConf.getDrmaaOptions();
    if (defaults.containsKey("mem")) {
      String mem = defaults.getString("mem");
      assert (!mem.startsWith("-X")) : "Error: mem parameter in .galago.conf file should not start with '-Xmx' or '-Xms'.";
      setMemoryUsage("-Xmx" + defaults.getString("mem"), "-Xms" + defaults.getString("mem"));
    }
    if (defaults.containsKey("nativeSpec")) {
      setNativeSpecification(defaults.getString("nativeSpec"));
    }
    if (defaults.containsKey("nativeSpecEach")) {
      nativeSpecification_each = nativeSpecification_each + " " + defaults.getString("nativeSpecEach");
    }
    if (defaults.containsKey("nativeSpecCombined")) {
      nativeSpecification_combined = nativeSpecification_combined + " " + defaults.getString("nativeSpecCombined");
    }

    // customize based upon arguments

    for (String arg : args) {
      if (arg.startsWith("Xmx")) {
        memory_x = "-" + arg;
      } else if (arg.startsWith("Xms")) {
        memory_s = "-" + arg;
      } else if (arg.startsWith("-t=")) {
        nodeTempDir = arg.replace("-t=", "");
      } else if (arg.startsWith("-v")) {
        verbose = true;
      } else if (arg.startsWith("-ns=")) {
        String ns = arg.replaceAll("^-ns=", "");
        // TODO(jfoley) find out what this does and if someone meant to catch the return value.
        // ns.replaceAll("(^\")|(\"$)", "");
        setNativeSpecification(ns);
      } else {
        System.out.println("Ignoring unknown argument: " + arg);
      }
    }

    try {
      session = SessionFactory.getFactory().getSession();
      session.init("");
    } catch (DrmaaException e) {
      logger.log(Level.WARNING, e.getMessage(), e);
    }
  }

  /**
   * <p>Sets a native specification.</p>
   *
   * @param nativeSpecification The specification to set.
   */
  public void setNativeSpecification(String nativeSpecification) {
    this.nativeSpecification_each = nativeSpecification_each + " " + nativeSpecification;
    this.nativeSpecification_combined = nativeSpecification_combined + " " + nativeSpecification;
  }

  /**
   * <p>Sets the initial heap space (<code>memory_s</code>) and the
   * maximum heap space (<code>memory_x</code>). See the values of
   * <code>MEMORY_X</code> and <code>MEMORY_S</code> for the defaults. </p>
   *
   * @param memory_x  The max heap space to use.
   * @param memory_s  The initial heap space to use.
   */
  public void setMemoryUsage(String memory_x, String memory_s) {
    this.memory_x = memory_x;
    this.memory_s = memory_s;
  }

  @Override
  public void shutdown() {
    try {
      session.exit();
    } catch (DrmaaException e) {
      logger.log(Level.WARNING, e.getMessage(), e);
    }
  }

  /**
   * <p>Submit all of the jobs for the given stage to the cluster.</p>
   *
   * @param stageName     The stage whose jobs are being submitted.
   * @param jobPaths      The path to each of the jobs being submitted.
   * @param temporary     The path to the temporary directory to use. This
   *                      is where the stdout/stderr files will be stored.
   *
   * @return The results of the jobs and eny errors that were thrown.
   */
  @Override
  public StageExecutionStatus submit(String stageName, ArrayList<String> jobPaths,
          String temporary) {
    ArrayList<String> jobs = new ArrayList<>();
    HashMap<String, File> jobCheckpoints = new HashMap<>();
    HashMap<String, Long> startTimes = new HashMap<>();
    try {
      // Cycle through each of the jobs for the given stage.
      for (int i = 0; i < jobPaths.size(); i++) {
        // Fill in the arguments to Java. These include the starting/max
        // heap space, the class path, the executor to call, and the
        // path to the job to run.
        String[] arguments = new String[]{"-ea", memory_x, memory_s,
          "-Djava.io.tmpdir=" + nodeTempDir,
          "-Dfile.encoding=" + DEFAULT_ENCODING,
          "-cp", classPath, className, jobPaths.get(i)};


        // Create the fill a DRMAA job template.
        JobTemplate template = session.createJobTemplate();
        template.setJobName("galago-" + stageName + "-" + i);
        template.setWorkingDirectory((new File(".")).getCanonicalPath());
        template.setRemoteCommand(command);
        template.setArgs(arguments);
        template.setOutputPath(":" + temporary + File.separator
                + "stdout");
        template.setErrorPath(":" + temporary + File.separator
                + "stderr");

        // If the user wants the jobs submitted to a particular
        // queue, set that here.
        if (jobPaths.size() == 1) {
          // if there's only one job - long queue
          if (nativeSpecification_combined.length() > 0) {
            template.setNativeSpecification(nativeSpecification_combined);
          }
        } else {
          // otherwise use the short queue
          if (nativeSpecification_each.length() > 0) {
            template.setNativeSpecification(nativeSpecification_each);
          }
        }

        // This will print the submitted command. (It gets kind of long
        // because of the class path and the absolute paths of the
        // job files.) We should probably have a switch for this...
        if (verbose) {
          System.err.print("Running: " + command);
          for (String argument : arguments) {
            System.err.print(" " + argument);
          }
        }


        // Run the job.
        String id = session.runJob(template);
        int status = session.getJobProgramStatus(id);
        if (status == Session.FAILED) {
          System.err.println("ERROR: Job failed! [" + stageName
                  + "-" + i + "]");
        }

        // Keep track of the job id.
        jobs.add(id);
        jobCheckpoints.put(id, new File(jobPaths.get(i) + ".complete"));

        startTimes.put(id, System.currentTimeMillis()); // for tracking

        // Clean up.
        session.deleteJobTemplate(template);
      }
    } catch (Exception e) {
      System.err.println("Problems submitting jobs: " + e.getMessage());
      return new DRMAAResult(stageName, jobs, jobCheckpoints, null, e);
    }

    System.err.println("job-launched: " + stageName);
    for (String id : jobs) {
      System.err.println("jobid: " + id);
    }
    return new DRMAAResult(stageName, jobs, jobCheckpoints, startTimes);
  }
}

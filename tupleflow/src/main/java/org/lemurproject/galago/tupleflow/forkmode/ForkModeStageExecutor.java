package org.lemurproject.galago.tupleflow.forkmode;

import org.lemurproject.galago.tupleflow.GalagoConf;
import org.lemurproject.galago.tupleflow.execution.CheckpointedStageExecutor;
import org.lemurproject.galago.tupleflow.execution.StageExecutionStatus;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

/**
 * ForkModeStageExecutor
 *
 * @author jfoley
 */
public class ForkModeStageExecutor extends CheckpointedStageExecutor {

    private static final Logger logger = Logger.getLogger(ForkModeStageExecutor.class.getName());

    // Flag to set the verbose mode (either on or off)
    public boolean verbose;

    // Use the 'java' specified in the env. variable JAVA_HOME --
    // we don't know what version of java the user called us with :(
    public String command = System.getenv("JAVA_HOME") + File.separator
            + "bin/java";
    // We do know what class path the user envoked us with, so use that
    // when submitting each of the jobs to the cluster.
    public String classPath = System.getProperty("java.class.path");
    // This is the TupleFlow executor we'll be using for each of the jobs.
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

    /**
     * <p>Creates a new ForkModeStageExecutor.</p>
     *
     * @param args An array; if it contains anything, the first
     *             element is used as the command when submitting
     *             jobs.
     */
    public ForkModeStageExecutor(String[] args) {
        assert (System.getenv("JAVA_HOME") != null);

        // Set the defaults for each job.
        setMemoryUsage(MEMORY_X, MEMORY_S);
        nodeTempDir = NODE_TEMP_DIR;
        verbose = false;

        Parameters defaults = GalagoConf.getMemoryOptions();
        if (defaults.containsKey("mem")) {
            String mem = defaults.getString("mem");
            assert (!mem.startsWith("-X")) : "Error: mem parameter in .galago.conf file should not start with '-Xmx' or '-Xms'.";
            setMemoryUsage("-Xmx" + defaults.getString("mem"), "-Xms" + defaults.getString("mem"));
        }

        // customize based upon arguments

        for (String arg : args) {
            if (arg.startsWith("Xmx")) {
                memory_x = "-" + arg;
            } else if (arg.startsWith("Xms")) {
                memory_s = "-" + arg;
            } else if (arg.startsWith("-t=")) {
                nodeTempDir = arg.replace("-t=", "");
            } else {
                System.out.println("Ignoring unknown argument: " + arg);
            }
        }

    }

    /**
     * <p>Sets the initial heap space (<code>memory_s</code>) and the
     * maximum heap space (<code>memory_x</code>). See the values of
     * <code>MEMORY_X</code> and <code>MEMORY_S</code> for the defaults. </p>
     *
     * @param memory_x The max heap space to use.
     * @param memory_s The initial heap space to use.
     */
    public void setMemoryUsage(String memory_x, String memory_s) {
        this.memory_x = memory_x;
        this.memory_s = memory_s;
    }

    @Override
    public void shutdown() {
    }

    /**
     * <p>Submit all of the jobs for the given stage to the current machine.</p>
     *
     * @param stageName The stage whose jobs are being submitted.
     * @param jobPaths  The path to each of the jobs being submitted.
     * @param temporary The path to the temporary directory to use. This
     *                  is where the stdout/stderr files will be stored.
     * @return The results of the jobs and eny errors that were thrown.
     */
    @Override
    public StageExecutionStatus submit(String stageName, ArrayList<String> jobPaths,
                                       String temporary) {
        ArrayList<ForkModeProcessRunner> jobs = new ArrayList<>();
        HashMap<String, File> jobCheckpoints = new HashMap<>();
        HashMap<String, Long> startTimes = new HashMap<>();
        try {
            // Cycle through each of the jobs for the given stage.
            for (int i = 0; i < jobPaths.size(); i++) {
                // Fill in the arguments to Java. These include the starting/max
                // heap space, the class path, the executor to call, and the
                // path to the job to run.
                List<String> arguments = Arrays.asList(
                        command,  // java
                        "-ea",  // assertions on
                        memory_x, memory_s, // memory settings
                        "-Djava.io.tmpdir=" + nodeTempDir, // tmp settings
                        "-Dfile.encoding=" + DEFAULT_ENCODING, // UTF-8
                        "-cp", classPath,  // cp reflected from current
                        ForkedLocalMain.class.getCanonicalName(), // class with main()
                        jobPaths.get(i) // args[0]
                );

                ProcessBuilder pb = new ProcessBuilder();
                pb.command(arguments);

                pb.redirectError(new File(jobPaths.get(i) + ".stderr"));
                pb.redirectOutput(new File(jobPaths.get(i) + ".stdout"));
                pb.directory(new File(".")); // transfer CWD
                String id = "galago-" + stageName + "-" + i;

                // This will print the submitted command. (It gets kind of long
                // because of the class path and the absolute paths of the
                // job files.) We should probably have a switch for this...
                if (verbose) {
                    System.err.print("Running: " + command);
                    for (String argument : arguments) {
                        System.err.print(" " + argument);
                    }
                }

                ForkModeProcessRunner runner = new ForkModeProcessRunner(id, pb);
                jobs.add(runner);
                startTimes.put(id, System.currentTimeMillis()); // for tracking
                jobCheckpoints.put(id, new File(jobPaths.get(i) + ".complete"));

                // start up a thread to babysit this process
                new Thread(runner).start();
            }
        } catch (Exception e) {
            System.err.println("Problems submitting jobs: " + e.getMessage());
            return new ForkModeResult(stageName, jobs, jobCheckpoints, null, e);
        }

        System.err.println("job-launched: " + stageName);
        for (ForkModeProcessRunner id : jobs) {
            System.err.println("jobid: " + id.getName());
        }
        return new ForkModeResult(stageName, jobs, jobCheckpoints, startTimes);
    }
}
/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools;

import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.JobExecutor;

import java.io.PrintStream;

/**
 * General interface for all galago applications.
 *
 * @author sjh
 */
public abstract class AppFunction {

  /**
   * returns the name of the application.
   */
  public abstract String getName();

  /**
   * returns a string detailing the usage and parameters of the function.
   */
  public abstract String getHelpString();

  /**
   * run the function using the input parameters, using 'output' to print the
   * required output data.
   */
  public abstract void run(Parameters p, PrintStream output) throws Exception;


  /**
   * If this is false, we will show help if not given any parameters
   */
  public boolean allowsZeroParameters() {
    return false;
  }

  /**
   * run the function by processing command line parameters, using 'output' to
   * print the required output data.
   *
   * This function can be overwritten to allow alternative parameter input
   * methods.
   */
  public void run(String[] args, PrintStream output) throws Exception {
    Parameters p = new Parameters();

    if (args.length == 1) {
      if(!allowsZeroParameters()) {
        output.print(this.getHelpString());
        return;
      }
    } else if (args.length > 1) {
      p = Parameters.parseArgs(Utility.subarray(args, 1));
      // don't want to wipe an existing parameter:
    }

    run(p, output);
  }

  /*
   * General helper functions for applications
   */
  public static String getTupleFlowParameterString() {
    return "Tupleflow Flags:\n"
            + "  --printJob={true|false}: Simply prints the execution plan of a Tupleflow-based job then exits.\n"
            + "                           [default=false]\n"
            + "  --mode={local|threaded|drmaa}: Selects which executor to use \n"
            + "                           [default=local]\n"
            + "  --port={int<65000} :     port number for web based progress monitoring. \n"
            + "                           [default=randomly selected free port]\n"
            + "  --galagoJobDir=/path/to/temp/dir/: Sets the galago temp dir \n"
            + "                           [default = uses folders specified in ~/.galagotmp or java.io.tmpdir]\n"
            + "  --deleteJobDir={true|false}: Selects to delete the galago job directory\n"
            + "                           [default = true]\n"
            + "  --distrib={int > 1}:     Selects the number of simultaneous jobs to create\n"
            + "                           [default = 10]\n"
            + "  --server={true|false}:   Selects to use a server to show the progress of a tupleflow execution.\n"
            + "                           [default = true]\n";
  }

  public static boolean runTupleFlowJob(Job job, Parameters p, PrintStream output) throws Exception {
    if (p.isBoolean("printJob") && p.getBoolean("printJob")) {
      p.remove("printJob");
      p.set("printJob", "dot");
    }

    String printJob = p.get("printJob", "none");
    if (printJob.equals("plan")) {
      output.println(job.toString());
      return true;
    } else if (printJob.equals("dot")) {
      output.println(job.toDotString());
      return true;
    }

    int hash = (int) p.get("distrib", 0);
    if (hash > 0) {
      job.properties.put("hashCount", Integer.toString(hash));
      // System.out.println(job.properties.get("hashCount"));
    }

    ErrorStore store = new ErrorStore();
    JobExecutor.runLocally(job, store, p);
    if (store.hasStatements()) {
      output.println(store.toString());
      return false;
    }
    return true;
  }
}

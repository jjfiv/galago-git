package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.JobExecutor;
import org.lemurproject.galago.utility.Parameters;

import java.io.PrintStream;

/**
 * @author jfoley.
 */
public class TupleflowAppUtil {
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

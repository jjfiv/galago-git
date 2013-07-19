/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class ChainFns extends AppFunction {

  @Override
  public String getName() {
    return "chain-jobs";
  }

  @Override
  public String getHelpString() {
    return "galago chain-jobs <input parameter file>\n\n"
            + " Runs multiple jobs in a chain. Looks for the\n"
            + " 'jobs' element in the parameter file. If the\n"
            + " value is an array, the jobs are run in serial.\n"
            + " If the value is a map, the jobs are runs as separate\n"
            + " child threads. Each inner job should have 'command'"
            + " and 'parameters' field.";
  }

  @Override
  public void run(Parameters jobs, PrintStream output) throws Exception {
    if (jobs.isList("jobs")) {
      List<Parameters> serialJobs = (List<Parameters>) jobs.getAsList("jobs");
      for (Parameters jobStep : serialJobs) {
        if (jobStep.get("active", true)) {
          Parameters stepParameters = findJobParameters(jobStep);
          String cmdName = jobStep.get("command", "none");
          if (!App.appFunctions.containsKey(cmdName)) {
            output.printf("Couldn't find command %s.\n", cmdName);
            return;
          }
          output.printf("Executing command: %s\n", cmdName);
          App.run(cmdName, stepParameters, output);
        }
      }
    } else if (jobs.isMap("jobs")) {
      output.printf("Not implemented yet. Sorry.\n");
    }
  }

  public Parameters findJobParameters(Parameters step) throws IOException {
    if (step.isString("parameters")) {
      File parameterPath = new File(step.getString("parameters"));
      if (!parameterPath.exists()) {
        throw new IOException(String.format("Unable to locate parameter file '%s'\n",
                step.getString("parameters")));
      }
      return Parameters.parseFile(parameterPath);
    } else if (step.isMap("parameters")) {
      return step.getMap("parameters");
    } else {
      throw new RuntimeException("No acceptable parameters found.");
    }
  }
}

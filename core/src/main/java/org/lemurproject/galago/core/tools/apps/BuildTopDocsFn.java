/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.tools.BuildTopDocs;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Job;

/**
 *
 * @author sjh
 */
public class BuildTopDocsFn extends AppFunction {

  @Override
  public String getName() {
    return "build-topdocs";
  }

  @Override
  public String getHelpString() {
    return "galago build-topdocs --index=<index> --part=<part> [--size=<size>] [--minLength=<minlength>]\n\n"
            + "  Constructs topdoc lists consisting of <size> documents,\n"
            + "  and only for lists longer than <minlength>. Note that\n"
            + "  <index> needs to point an index, while <part> is the part to scan.\n";
  }

  @Override
  public void run(String[] args, PrintStream output) throws Exception {
    if (args.length < 3) {
      output.println(getHelpString());
      return;
    }

    Parameters p = new Parameters(Utility.subarray(args, 1));
    assert (p.isString("index"));
    assert (p.isString("part"));
    // assert(p.isLong("size"));
    // assert(p.isLong("minLength"));
    run(p, output);
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    BuildTopDocs build = new BuildTopDocs();
    Job job = build.getIndexJob(p);
    runTupleFlowJob(job, p, output);
  }
}

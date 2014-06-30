/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import java.io.PrintStream;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class LearnQueryParameters extends AppFunction {

  @Override
  public String getName() {
    return "learner";
  }

  @Override
  public String getHelpString() {
    return "galago learn <<see learner.java for parameters>>\n\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    // should check parameters here.
    Retrieval retrieval = RetrievalFactory.instance(p);
    Learner learner = LearnerFactory.instance(p, retrieval);
    try {
      RetrievalModelInstance tunedParameters = learner.learn();
      Parameters instParams = tunedParameters.toParameters();
      output.println(instParams.toString());
    } finally {
      // ensure all buffered streams are correctly flushed.
      learner.close();
    }
  }

  public static void main(String[] args) throws Exception {
    Parameters p = Parameters.parseArgs(args);
    new LearnQueryParameters().run(p, System.out);
  }
}

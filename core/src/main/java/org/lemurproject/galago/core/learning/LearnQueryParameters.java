/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning;

import java.io.PrintStream;
import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.tools.App.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class LearnQueryParameters extends AppFunction {

  @Override
  public String getHelpString() {
    return "galago learn --parameters--";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    // should check parameters here.

    Retrieval retrieval = RetrievalFactory.instance(p);
    Learner learner = LearnerFactory.instance(p, retrieval);
    List<Parameters> tunedParameters = learner.learn();
    for(int run = 0; run < tunedParameters.size(); run++){
      output.print(run);
      output.print("\t");
      output.println(tunedParameters.get(run));
    }
  }

  public static void main(String[] args) throws Exception {
    Parameters p = new Parameters();
    p = new Parameters(args);
    new LearnQueryParameters().run(p, System.out);
  }
}

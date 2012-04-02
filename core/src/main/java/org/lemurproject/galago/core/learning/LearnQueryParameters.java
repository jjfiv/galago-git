/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning;

import java.io.IOException;
import java.io.PrintStream;
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

    Learner learner = LearnerFactory.instance(p);
    Parameters tunedParameters = learner.learn();
  }

  public static void main(String[] args) throws Exception {
    Parameters p = new Parameters();
    p = new Parameters(args);
    new LearnQueryParameters().run(p, System.out);
  }
}

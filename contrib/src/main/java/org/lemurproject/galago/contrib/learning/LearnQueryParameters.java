/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import java.io.PrintStream;
import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class LearnQueryParameters extends AppFunction {

  @Override
  public String getName(){
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
    List<RetrievalModelInstance> tunedParameters = learner.learn();
    for(int id = 0; id < tunedParameters.size(); id++){
      RetrievalModelInstance instance = tunedParameters.get(id);
      Parameters instParams = instance.toParameters();
      output.println(instParams.toString());
    }
  }

  public static void main(String[] args) throws Exception {
    Parameters p = new Parameters(args);
    new LearnQueryParameters().run(p, System.out);
  }
}

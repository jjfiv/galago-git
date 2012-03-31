/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning;

import java.io.PrintStream;
import java.util.List;
import org.lemurproject.galago.core.eval.QuerySetJudgments;
import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluator;
import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluatorFactory;
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
    return "galago learn <parameters> <queries> <qrels> <index>";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    // should check parameters here.
    
    Learner learner = LearnerFactory.instance(p);
    Parameters tunedParameters = learner.learn();
    
    
  }
}

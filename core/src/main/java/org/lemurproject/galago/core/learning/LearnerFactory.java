/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class LearnerFactory {

  public static Learner instance(Parameters p, Retrieval retrieval) throws Exception {
    if(p.get("xfolds", 1) > 1){
      return new XFoldLearner(p, retrieval);
    } else {
      return new CoordinateAscentLearner(p, retrieval);
    }
  }
  
}

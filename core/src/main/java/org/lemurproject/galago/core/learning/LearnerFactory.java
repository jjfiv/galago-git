/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning;

import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class LearnerFactory {

  public static Learner instance(Parameters p) throws Exception {
    return new CoordinateAscent(p);
  }
  
}

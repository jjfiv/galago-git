/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class LearnerFactory {
  
  static HashMap<String, Class> learnerLookup = new HashMap();
  
  static {
    learnerLookup.put("coord", CoordinateAscentLearner.class);
    learnerLookup.put("grid", GridSearchLearner.class);
    learnerLookup.put("xfold", XFoldLearner.class);
    // otherwise by default use coordinate ascent
    learnerLookup.put("default", CoordinateAscentLearner.class);
  }
  
  public static Learner instance(Parameters p, Retrieval retrieval) throws Exception {
    if (p.containsKey("learner") && !p.isString("learner")) {
      throw new RuntimeException("Learner factory requires a string parameter: learner");
    }
    if (!learnerLookup.containsKey(p.get("learner", "default"))) {
      throw new RuntimeException("Learner factory can not create learner: " + p.getString("learner"));
    }
    
    Class learner = learnerLookup.get(p.get("learner", "default"));

    // there is currently only one possible constructor: \w (Parameters, Retrieval)
    Constructor cons = learner.getConstructor(Parameters.class, Retrieval.class);
    
    if (cons != null) {
      // cast is safe - because only 'learner' classes should be in the lookup
      return (Learner) cons.newInstance(new Object[]{p, retrieval});
    } else {
      return null;
    }
  }
}

/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.prf;

import java.lang.reflect.Constructor;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class ExpansionModelFactory {

  /** @deprecated use create instead! */
  @Deprecated
  public static ExpansionModel instance(Parameters parameters, Retrieval retrieval) throws Exception {
    return create(parameters, retrieval);
  }
  public static ExpansionModel create(Parameters parameters, Retrieval retrieval) throws Exception {
    if (parameters.isString("relevanceModel")) {
      Class clazz = Class.forName(parameters.getString("relevanceModel"));
      Constructor cons = clazz.getConstructor(Retrieval.class);
      ExpansionModel em = (ExpansionModel) cons.newInstance(retrieval);
      return em;
    
    } else {
      return new RelevanceModel3(retrieval);
      
    }
  }
}

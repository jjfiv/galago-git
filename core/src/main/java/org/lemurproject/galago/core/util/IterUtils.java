package org.lemurproject.galago.core.util;

import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.utility.Parameters;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jfoley.
 */
public class IterUtils {
  /**
   * Adds an operator into a Retrieval's parameters for usage.
   * @param p the parameters object
   * @param name the name of the operator, e.g. "combine" for #combine
   * @param iterClass the operator to register
   */
  public static void addToParameters(Parameters p, String name, Class<? extends BaseIterator> iterClass) {
    if(!p.containsKey("operators")) {
      p.put("operators", Parameters.create());
    }
    p.getMap("operators").put(name, iterClass.getName());
  }

  /**
   * Adds a traversal into a Retrieval's parameters for usage.
   * @param argp the parameters object
   * @param traversalClass the traversal to register
   */
  public static void addToParameters(Parameters argp, Class<? extends Traversal> traversalClass) {
    if(!argp.isList("traversals")) {
      argp.put("traversals", new ArrayList<>());
    }
    List<Parameters> traversals = argp.getList("traversals", Parameters.class);
    traversals.add(Parameters.parseArray(
      "name", traversalClass.getName(),
      "order", "before"
    ));
    argp.put("traversals", traversals);
  }
}

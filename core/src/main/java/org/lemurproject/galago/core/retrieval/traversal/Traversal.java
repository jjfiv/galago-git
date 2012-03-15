// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Basic interface for Traversals.
 *
 * [irmarc,2/23/2012] - Modified to an abstract class. In order to reduce the workload of
 *               executing all the traversals, we can check if they *need* to be run
 *               at all. Default behavior is yes, however if you can provide a no (and some
 *               of them do this easily), then we can cut out no-effect traversal walks.
 *
 * @author trevor, irmarc
 */
public abstract class Traversal {
  
  public abstract Node afterNode(Node newNode) throws Exception;

  public abstract void beforeNode(Node object) throws Exception;

  /**
   * True if the traversal in question needs to be executed. Let's see if this
   * works.
   * 
   * @param root
   * @return
   * @throws Exception
   */
  public static boolean isNeeded(Node root) {
    return true;
  }
}

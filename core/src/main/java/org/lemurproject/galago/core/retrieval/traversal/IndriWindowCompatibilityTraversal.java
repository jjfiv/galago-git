// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Looks at the current node and attempts to rewrite Indri-style
 * operators in the Galago format.  It can rewrite three types of expressions:
 * <ul>
 *  <li>#<i>n</i> changes to #od:</i>n</i></li>
 *  <li>#od<i>n</i> changes to #od:<i>n</i></li>
 *  <li>#uw<i>n</i> changes to #uw:<i>n</i></li>
 * </ul>
 * @author trevor
 */
public class IndriWindowCompatibilityTraversal extends Traversal {

  public Node afterNode(Node original) {
    String operator = original.getOperator();

    if (operator.length() == 0) {
      return original;
    }

    if (Character.isDigit(operator.codePointAt(0))) {
      // this is a #n node, which is an ordered window node
      original.getNodeParameters().set("default", Long.parseLong(operator));
      original.setOperator("od");
      return original;
    } else if ((operator.startsWith("od") || (operator.startsWith("uw")))
            && operator.length() > 2
            && Character.isDigit(operator.codePointAt(2))) {
      // this is an #od3() or #uw4 node (op followed by digits)
      original.getNodeParameters().set("default", Long.parseLong(operator.substring(2)));
      original.setOperator(operator.substring(0, 2));
      return original;
    }

    // No changes
    return original;
  }

  public void beforeNode(Node object) throws Exception {
    // does nothing
  }
}

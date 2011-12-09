// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

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
public class IndriWindowCompatibilityTraversal implements Traversal {
    public IndriWindowCompatibilityTraversal(Retrieval retrieval) {
    }

    public Node afterNode(Node original) {
        String operator = original.getOperator();
        List<Node> children = Node.cloneNodeList(original.getInternalNodes());

        if (operator.length() == 0) {
            return original;
        }

        if (Character.isDigit(operator.codePointAt(0))) {
            // this is a #n node, which is an ordered window node
            return new Node("od", new NodeParameters(Long.parseLong(operator)), children, original.getPosition());
        } else if (operator.startsWith("od") &&
                operator.length() > 2 &&
                Character.isDigit(operator.codePointAt(2))) {
            // this is a #od3() node
            return new Node("od", new NodeParameters(Long.parseLong(operator.substring(2))),
                    children, original.getPosition());
        } else if (operator.startsWith("uw") &&
                operator.length() > 2 &&
                Character.isDigit(operator.codePointAt(2))) {
            // this is a #uw3 node
            return new Node("uw", new NodeParameters(Long.parseLong(operator.substring(2))),
                    children, original.getPosition());
        }

        return original;
    }

    public void beforeNode(Node object) throws Exception {
        // does nothing
    }
}

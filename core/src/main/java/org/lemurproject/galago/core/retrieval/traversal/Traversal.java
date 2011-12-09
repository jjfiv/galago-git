// BSD License (http://lemurproject.org/galago-license)


package org.lemurproject.galago.core.retrieval.traversal;

import java.util.ArrayList;
import org.lemurproject.galago.core.retrieval.query.Node;

/**
 * Basic interface for Traversals.
 *
 * @author trevor
 */
public interface Traversal {
    public Node afterNode(Node newNode) throws Exception;
    public void beforeNode(Node object) throws Exception;
}
    

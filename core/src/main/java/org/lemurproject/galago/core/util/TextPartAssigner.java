// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.util;

import java.util.Set;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.tartarus.snowball.ext.englishStemmer;

/**
 *
 * @author irmarc
 */
public class TextPartAssigner {

  public static Node assignPart(Node original, Parameters parts) {
    Set<String> available = parts.getKeys();
    if (available.contains("postings.porter")) {
      return transformedNode(original, "postings.porter", "extents");
    } else if (available.contains("postings.krovetz")) {
      return transformedNode(original, "postings.krovetz", "extents");
    } else if (available.contains("postings")) {
      return transformedNode(original, "postings", "extents");
    } else {
      return original;
    }
  }

  public static Node transformedNode(Node original,
          String indexName, String operatorName) {
    NodeParameters parameters = original.getNodeParameters();
    parameters.set("part", indexName);
    return new Node(operatorName, parameters, Node.cloneNodeList(original.getInternalNodes()), original.getPosition());
  }
}

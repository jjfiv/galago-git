// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.util;

import java.io.IOException;
import java.util.Set;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
public class TextPartAssigner {

  public static Node assignPart(Node original, Parameters globalParams, Parameters availableParts) throws IOException {
    if (original.getNodeParameters().isString("part")) {
      return original;
    } else if (availableParts.isString("defaultTextPart")) {
      return transformedNode(original, availableParts.getString("defaultTextPart"));
    } else if (globalParams.isString("defaultTextPart")) {
      return transformedNode(original, globalParams.getString("defaultTextPart"));
    } else {
      Set<String> available = availableParts.getKeys();
      if (available.contains("postings.porter")) {
        return transformedNode(original, "postings.porter");
      } else if (available.contains("postings.krovetz")) {
        return transformedNode(original, "postings.krovetz");
      } else if (available.contains("postings")) {
        return transformedNode(original, "postings");
      } else {
        return original;
      }
    }
  }

  public static Node transformedNode(Node original, String indexName) {
    NodeParameters parameters = original.getNodeParameters();
    parameters.set("part", indexName);
    return original;
  }
}

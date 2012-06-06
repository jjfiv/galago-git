// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.util;

import java.io.IOException;
import java.util.Set;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
public class TextPartAssigner {

  public static Node assignPart(Node original, Retrieval retrieval, Parameters queryParams) throws IOException {
    if (original.getNodeParameters().isString("part")) {
      return original;
    } else if (queryParams.isString("defaultTextPart")) {
      return transformedNode(original, queryParams.getString("defaultTextPart"));
    } else if (retrieval.getGlobalParameters().isString("defaultTextPart")) {
      return transformedNode(original, retrieval.getGlobalParameters().getString("defaultTextPart"));
    } else {
      Set<String> available = retrieval.getAvailableParts().getKeys();
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

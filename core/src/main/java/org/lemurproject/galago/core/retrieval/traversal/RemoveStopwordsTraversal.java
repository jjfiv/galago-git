// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.File;
import java.io.IOException;
import java.lang.String;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Finds stopwords in a query and removes them.  This does not
 * attempt to remove stopwords from phrase operators.
 * 
 * @author trevor
 */
public class RemoveStopwordsTraversal extends Traversal {

  HashSet<String> words;

  public RemoveStopwordsTraversal(Retrieval retrieval) {
    Parameters parameters = retrieval.getGlobalParameters();
    // Look for a file first
    if (parameters.containsKey("stopwords")) {
      if (parameters.isString("stopwords")) {
        File f = new File(parameters.getString("stopwords"));
        if (f.exists()) {
          try {
            words = Utility.readFileToStringSet(f);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        }
      } else {
        List<String> wordsList = parameters.getList("stopwords");
        words = new HashSet<String>(wordsList);
      }
    } else {
      words = new HashSet<String>();
    }
  }

  @Override
  public Node afterNode(Node node) throws Exception {

    // if the node is a stopword - replace with 'null' operator
    if ((node.getOperator().equals("counts")
            || node.getOperator().equals("extents"))
            && words.contains(node.getDefaultParameter())) {
      return new Node("null", new ArrayList());
    }

    // otherwise return the original
    return node;
  }

  @Override
  public void beforeNode(Node node) throws Exception {
    // nothing
  }
}

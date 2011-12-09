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
public class RemoveStopwordsTraversal implements Traversal {

  HashSet<String> words;
  HashSet<String> conjops;

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

    conjops = new HashSet();
    conjops.add("inside");
    conjops.add("ordered");
    conjops.add("od");
    conjops.add("unordered");
    conjops.add("uw");
    conjops.add("all");
  }

  public Node afterNode(Node node) throws Exception {

    // if the node is a stopword - replace with 'null' operator
    if ((node.getOperator().equals("counts")
            || node.getOperator().equals("extents"))
            && words.contains(node.getDefaultParameter())) {
      return new Node("null", new ArrayList());
    }

    // now if we have a conjunction node, we need to remove any null op children.
    List<Node> children = node.getInternalNodes();
    ArrayList<Node> newChildren = new ArrayList();
    for (Node child : children) {
      if (!child.getOperator().equals("null")) {
        newChildren.add(child);
      }
    }

    boolean hasNull = children.size() > newChildren.size();

    if (hasNull && conjops.contains(node.getOperator())) {
      // special case: inside 
      if (node.getOperator().equals("inside")) {
        return new Node("null", new ArrayList());
      }

      // all other cases - create a new list of non-null children
      if (newChildren.size() == 0) {
        return new Node("null", new ArrayList());
      } else {
        return new Node(node.getOperator(), node.getNodeParameters(), Node.cloneNodeList(newChildren), node.getPosition());
      }
    }

    // otherwise return the original
    return node;
  }

  public void beforeNode(Node node) throws Exception {
    // nothing
  }
}

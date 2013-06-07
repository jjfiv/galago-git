// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.util.WordLists;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Removes stopwords from query 
 * 
 * e.g:
 *
 * #stopword ( #sdm ( #text:this #text:example #text:query ) ) --> #combine ( #sdm (
 * #text:example #text:query ) )
 *
 * (later) #combine ( #combine ( #combine( unigrams ) #combine (bigrams)
 * #combine(uws) ))
 *
 * or.
 *
 * #ss ( #text:this #text:example #text:query ) --> #combine ( #text:example
 * #text:query )
 *
 * @author sjh, jbing
 */
public class StopWordTraversal extends Traversal {

  public static Set<String> defaultStopwords = null;

  public StopWordTraversal(Retrieval retrieval) throws IOException {
    if (defaultStopwords == null) {
      // default to 'inquery' list
      String stopwordlist = retrieval.getGlobalParameters().get("stopwordlist", "inquery");
      defaultStopwords = WordLists.getWordList(stopwordlist);
    }
  }

  @Override
  public void beforeNode(Node node, Parameters queryParameters) throws Exception {
  }

  @Override
  public Node afterNode(Node original, Parameters queryParameters) throws Exception {
    Set<String> stopwords = defaultStopwords;
    if (queryParameters.isString("stopwordlist")) {
      String stopwordlist = queryParameters.getString("stopwordlist");
      stopwords = WordLists.getWordList(stopwordlist);
    }
    if (original.getOperator().equals("stopword")) {
      // remove #stopword from node
      Node newHead = new Node("combine", original.getInternalNodes());

      // recusively find and remove stopwords from #text nodes
      recFindStopWords(newHead, stopwords);

      return newHead;
    }
    return original;
  }

  private void recFindStopWords(Node node, Set<String> stopwords) {
    if (node.getOperator().equals("text")) {
      // check for stopword, delete node if nec.
      String term = node.getDefaultParameter();
      if (stopwords.contains(term)) {
        Node parent = node.getParent();
        parent.removeChild(node);
      }

    } else {
      List<Node> childrenCopy = new ArrayList(node.getInternalNodes());
      for (Node child : childrenCopy) {
        recFindStopWords(child, stopwords);
      }
    }
  }
}

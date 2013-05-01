/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.HashMap;
import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Allows direct replacement of operators in queries.
 *  - allows a single query file to be used for several
 *    different retrieval models:
 * 
 * opRepls : { "dummy" : "sdm" } -->
 * #dummy( t t t ) --> #sdm(t t t)
 * 
 * opRepls : { "dummy" : ["stopword", "combine"] } -->
 * #dummy( t t t ) --> #stopword( #combine( t t t ) )
 *
 * @author sjh
 */
public class ReplaceOperatorTraversal extends Traversal {

  Parameters operators;

  public ReplaceOperatorTraversal(Retrieval ret) {
    Parameters p = ret.getGlobalParameters();

    operators = p.isMap("opRepls") ? p.getMap("opRepls") : new Parameters();
  }

  @Override
  public Node afterNode(Node original, Parameters p) throws Exception {
    
    // overrides globals -- could cause problems.
    Parameters instOperators = p.isMap("opRepls") ? p.getMap("opRepls") : operators;
    
    String key = original.getOperator();
    if (instOperators.containsKey(key)) {
      switch (instOperators.getKeyType(key)) {
        case STRING:
          original.setOperator(instOperators.getString(key));
          return original;

        case LIST:
          if (instOperators.isList(key, Parameters.Type.STRING)) {
            List<String> repls = (List<String>) instOperators.getList(key);
            Node root = null;
            Node curr = null;
            for (String r : repls) {
              if (root == null) {
                curr = new Node(r);
                root = curr;
              } else {
                curr.addChild(new Node(r));
                curr = curr.getChild(0);
              }
            }
            for (Node c : original.getInternalNodes()) {
              curr.addChild(c);
            }
            return root;
          }
        default:
          throw new IllegalArgumentException("--opReps mapping must contain only Strings or lists of Strings.");
      }
    }
    return original;
  }

  @Override
  public void beforeNode(Node object, Parameters queryParams) throws Exception {
  }
}

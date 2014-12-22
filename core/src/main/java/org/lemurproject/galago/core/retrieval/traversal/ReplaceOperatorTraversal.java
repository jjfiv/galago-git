/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.Parameters;

import java.util.List;

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
    this(ret.getGlobalParameters());
  }
  
  public ReplaceOperatorTraversal(Parameters p) {
    operators = p.isMap("opRepls") ? p.getMap("opRepls") : Parameters.create();
  }

  @Override
  public Node afterNode(Node original, Parameters p) throws Exception {
    
    // overrides globals -- could cause problems.
    Parameters instOperators = p.isMap("opRepls") ? p.getMap("opRepls") : operators;
    
    String key = original.getOperator();
    if (instOperators.containsKey(key)) {
      if(instOperators.isString(key)) {
        original.setOperator(instOperators.getString(key));
        return original;
      } else if(instOperators.isList(key)) {
        if (instOperators.isList(key, String.class)) {
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
      } else {
          throw new IllegalArgumentException("--opReps mapping must contain only Strings or lists of Strings.");
      }
    }
    return original;
  }

  @Override
  public void beforeNode(Node object, Parameters queryParams) throws Exception {
  }
}

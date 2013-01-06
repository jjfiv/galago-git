/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import java.util.HashMap;
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
 * opRepls : { "dummy" : "combine" } -->
 * #dummy( t t t ) --> #combine(t t t)
 *
 * @author sjh
 */
public class ReplaceOperatorTraversal extends Traversal {

  HashMap<String,String> operators;
  
  public ReplaceOperatorTraversal(Retrieval ret, Parameters queryParams){
    operators = new HashMap();
    Parameters repls = ret.getGlobalParameters().get("opRepls", new Parameters());
    repls.copyFrom( queryParams.get("opRepls", new Parameters()) );
    for(String key : repls.getKeys()){
      operators.put(key, repls.getString(key));
    }
    
  }
  
  @Override
  public Node afterNode(Node original) throws Exception {
    if(operators.containsKey(original.getOperator())){
      original.setOperator(operators.get(original.getOperator()));
    }
    return original;
  }

  @Override
  public void beforeNode(Node object) throws Exception {
  }  
}

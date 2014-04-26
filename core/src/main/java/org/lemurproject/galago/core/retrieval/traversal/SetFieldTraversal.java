package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * @author jfoley
 */
public class SetFieldTraversal extends Traversal {
  @Override
  public void beforeNode(Node original, Parameters queryParameters) throws Exception {
  }

  @Override
  public Node afterNode(Node original, Parameters queryParameters) throws Exception {
    String field = queryParameters.get("setField", "");
    String stemmer = queryParameters.get("stemmer", "krovetz");

    // build up part name by using an uncomfortable amount of knowledge about the guts of retrievals...
    String partName = "field";
    if(field.isEmpty())
      partName = "postings";
    if(!stemmer.isEmpty())
      partName += "."+stemmer;
    if(!field.isEmpty())
      partName += "."+field;

    String operator = original.getOperator();
    if(operator.equals("extents") || operator.equals("counts")) {
      original.getNodeParameters().set("part", partName);
    } else if(operator.equals("lengths")) {
      original.getNodeParameters().set("default", field);
    }
    return original;
  }
}

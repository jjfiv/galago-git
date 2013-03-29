// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Transforms an #inside operator into a
 *  #extents operator using the field parts.
 *
 *
 * @author sjh
 */
public class InsideToFieldPartTraversal extends Traversal {

  Parameters availableParts;
  private Parameters globalParameters;

  public InsideToFieldPartTraversal(Retrieval retrieval) throws IOException {
    this.availableParts = retrieval.getAvailableParts();
    globalParameters = retrieval.getGlobalParameters();
  }

  @Override
  public void beforeNode(Node original) throws Exception {
  }

  @Override
  public Node afterNode(Node original) throws Exception {
    if (original.getOperator().equals("inside")) {
      // a way out - in case you really want it.
      if (original.getNodeParameters().get("noOpt", false)) {
        return original;
      }

      List<Node> children = original.getInternalNodes();
      if (children.size() != 2) {
        return original;
      }
      Node text = children.get(0);
      Node field = children.get(1);

      assert (text.getOperator().equals("extents")
              && text.getNodeParameters().isString("part"));
      assert (field.getOperator().equals("extents")
              && field.getNodeParameters().isString("part"));

      String fieldPart = text.getNodeParameters().getString("part").replaceFirst("postings", "field") + "." + field.getDefaultParameter();

      if (!availableParts.containsKey(fieldPart)) {
        return original;
      }

      Node n = TextPartAssigner.transformedNode(text.clone(), fieldPart);
      n.setOperator("extents");
      return n;
    } else {
      return original;
    }
  }
}

// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.utility.Parameters;

/**
 * <p>StructuredQuery.parse parses queries using pseudo-operators, like #text
 * and #field, so that <tt>a.b.</tt> becomes <tt>#inside( #text:a() #field:b()
 * )</p>. These pseudo-operators are not supported by common index types. This
 * traversal renames <tt>#text</tt> and
 * <tt>#field</tt> to something sensible.</p>
 *
 * @author trevor
 */
public class TextFieldRewriteTraversal extends Traversal {

  Parameters availableParts;
  Parameters globalParams;
  Retrieval retrieval;

  public TextFieldRewriteTraversal(Retrieval retrieval) throws IOException {
    this.retrieval = retrieval;
    this.availableParts = retrieval.getAvailableParts();
    this.globalParams = retrieval.getGlobalParameters();
  }

  @Override
  public void beforeNode(Node object, Parameters qp) throws Exception {
    // do nothing
  }

  @Override
  public Node afterNode(Node original, Parameters qp) throws Exception {
    String operator = original.getOperator();

    // TODO: use qp to override globals
    if (operator.equals("text")) {
      return TextPartAssigner.assignPart(new Node("extents", original.getNodeParameters()), globalParams, retrieval.getAvailableParts());

    } else if (operator.equals("field")) {
      if (availableParts.getKeys().contains("extents")) {
        Node n = TextPartAssigner.transformedNode(original, "extents");
        n.setOperator("extents");
        return n;
      } else {
        return original;
      }
    } else {
      return original;
    }
  }
}

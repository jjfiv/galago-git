// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.tupleflow.Parameters;

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

  public void beforeNode(Node object) throws Exception {
    // do nothing
  }

  public Node afterNode(Node original) throws Exception {
    String operator = original.getOperator();

    if (operator.equals("text")) {
      return TextPartAssigner.assignPart(new Node("extents", original.getNodeParameters()),
              retrieval.getGlobalParameters(), retrieval.getAvailableParts());
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

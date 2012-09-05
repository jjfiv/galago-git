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
 * traversal renames <tt>#text</tt> and <tt>#field</tt> to something
 * sensible.</p>
 *
 * @author trevor
 */
public class TextFieldRewriteTraversal extends Traversal {

  Parameters queryParams;
  Retrieval retrieval;

  public TextFieldRewriteTraversal(Retrieval retrieval, Parameters queryParams) throws IOException {
    this.retrieval = retrieval;
    this.queryParams = queryParams;
  }

  @Override
  public void beforeNode(Node object) throws Exception {
    // do nothing
  }

  @Override
  public Node afterNode(Node original) throws Exception {
    String operator = original.getOperator();

    if (operator.equals("text")) {
      return TextPartAssigner.assignPart(new Node("extents", original.getNodeParameters()), 
              retrieval.getGlobalParameters(), retrieval.getGlobalParameters());
    } else if (operator.equals("field")) {
      if (retrieval.getAvailableParts().getKeys().contains("extents")) {
        return TextPartAssigner.transformedNode(new Node("extents", original.getNodeParameters()), "extents");
      }
    }
    return original;
  }
}

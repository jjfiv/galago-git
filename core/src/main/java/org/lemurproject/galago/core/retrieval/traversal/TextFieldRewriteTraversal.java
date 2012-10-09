// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * <p>StructuredQuery.parse parses queries using pseudo-operators, like #text and #field, so
 * that <tt>a.b.</tt> becomes <tt>#inside( #text:a() #field:b() )</p>.  These pseudo-operators
 * are not supported by common index types.  This traversal renames <tt>#text</tt> and
 * <tt>#field</tt> to something sensible.</p>
 * 
 * @author trevor
 */
public class TextFieldRewriteTraversal extends Traversal {

  Parameters availableParts;
  Parameters globalParams;

  public TextFieldRewriteTraversal(Retrieval retrieval) throws IOException {
    this.availableParts = retrieval.getAvailableParts();
    this.globalParams = retrieval.getGlobalParameters();
  }

  public void beforeNode(Node object) throws Exception {
    // do nothing
  }

  public Node afterNode(Node original) throws Exception {
    String operator = original.getOperator();

    if (operator.equals("text")) {
      if (globalParams.containsKey("mod")) {
        original.getNodeParameters().set("mod", globalParams.getString("mod"));
      }
      return TextPartAssigner.assignPart(original, availableParts, globalParams);
    } else if (operator.equals("field")) {
      if (availableParts.getKeys().contains("extents")) {
        return TextPartAssigner.transformedNode(original, "extents", "extents");
      } else {
        return original;
      }
    } else {
      return original;
    }
  }
}

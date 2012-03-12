/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal.optimize;

import org.lemurproject.galago.core.retrieval.traversal.optimize.FlattenWindowTraversal;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;

/**
 *
 * @author sjh
 */
public class FlattenWindowTraversalTest extends TestCase {

  public FlattenWindowTraversalTest(String testName) {
    super(testName);
  }

  public void testNestedWindowRewrite() throws Exception {
    String query = "#uw:5( #od:1(#text:a() #text:b()) )";
    Node result = StructuredQuery.parse(query);
    Node transformed = StructuredQuery.copy(new FlattenWindowTraversal(), result);
    assertEquals("#od:1( #text:a() #text:b() )", transformed.toString());
  }
}

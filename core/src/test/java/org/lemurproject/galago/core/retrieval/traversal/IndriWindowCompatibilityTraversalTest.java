/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;


import org.lemurproject.galago.core.retrieval.traversal.IndriWindowCompatibilityTraversal;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;

/**
 *
 * @author sjh
 */
public class IndriWindowCompatibilityTraversalTest extends TestCase {

  public IndriWindowCompatibilityTraversalTest(String testName) {
    super(testName);
  }

  public void testIndriPoundNRewrite() throws Exception {
    String query = "#3()";
    Node result = StructuredQuery.parse(query);
    
    Node transformed = StructuredQuery.copy(new IndriWindowCompatibilityTraversal(), result);
    assertEquals("#od:3()", transformed.toString());
  }

  public void testIndriOdNRewrite() throws Exception {
    String query = "#od3()";
    Node result = StructuredQuery.parse(query);
    Node transformed = StructuredQuery.copy(new IndriWindowCompatibilityTraversal(), result);
    assertEquals("#od:3()", transformed.toString());
  }

  public void testIndriUwNRewrite() throws Exception {
    String query = "#uw5()";
    Node result = StructuredQuery.parse(query);
    Node transformed = StructuredQuery.copy(new IndriWindowCompatibilityTraversal(), result);
    assertEquals("#uw:5()", transformed.toString());
  }
}

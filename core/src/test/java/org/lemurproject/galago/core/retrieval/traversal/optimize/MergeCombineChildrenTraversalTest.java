/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal.optimize;

import org.lemurproject.galago.core.retrieval.traversal.optimize.MergeCombineChildrenTraversal;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;

/**
 *
 * @author sjh
 */
public class MergeCombineChildrenTraversalTest extends TestCase {
  
  public MergeCombineChildrenTraversalTest(String testName) {
    super(testName);
  }

  public void testNestedCombineMerger() throws Exception {
    String query = "#combine:3=0.7:2=0.3:1=0.8:0=0.2( #ow:1(#text:a() #text:b()) #text:b() #ow:1(#text:a() #text:b()) #text:d() )";
    Node result = StructuredQuery.parse(query);
    Node transformed = StructuredQuery.copy(new MergeCombineChildrenTraversal(null), result);
    assertEquals("#combine:0=0.5:1=0.8:2=0.7( #ow:1( #text:a() #text:b() ) #text:b() #text:d() )", transformed.toString());
  }
}

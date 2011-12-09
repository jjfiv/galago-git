/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal.optimize;

import org.lemurproject.galago.core.retrieval.traversal.optimize.FlattenCombineTraversal;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;

/**
 *
 * @author sjh
 */
public class FlattenCombineTraversalTest extends TestCase {

  public FlattenCombineTraversalTest(String testName) {
    super(testName);
  }

  public void testNestedCombineMerger() throws Exception {
    String query = "#combine(#combine:0=0.1:1=0.4(#text:a() #text:b()) #combine:0=150:1=350(#text:c() #text:d()))";
    Node result = StructuredQuery.parse(query);
    Node transformed = StructuredQuery.copy(new FlattenCombineTraversal(null), result);
    assertEquals("#combine:0=0.2:1=0.8:2=0.3:3=0.7( #text:a() #text:b() #text:c() #text:d() )", transformed.toString());
  }
}

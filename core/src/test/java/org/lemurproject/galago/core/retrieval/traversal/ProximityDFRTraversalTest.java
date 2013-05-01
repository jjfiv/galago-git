/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class ProximityDFRTraversalTest extends TestCase {

  public ProximityDFRTraversalTest(String testName) {
    super(testName);
  }

  public void testSomeMethod() throws Exception {
    Parameters qparams = new Parameters();
    Retrieval ret = new FakeRetrieval();

    ProximityDFRTraversal traversal = new ProximityDFRTraversal(ret);

    Node pl2root = StructuredQuery.parse("#pdfr( test query )");
    Node out = traversal.afterNode(pl2root, qparams);
    Node exp = StructuredQuery.parse("#combine:0=1.0:1=1.0:norm=false( #combine( "
            + "#feature:bil2:c=6.0( #lengths:document:part=lengths() #text:test() ) "
            + "#feature:bil2:c=6.0( #lengths:document:part=lengths() #text:query() ) ) "
            + "#combine( "
            + "#feature:bil2:c=0.05( #lengths:document:part=lengths() #ordered:5( #text:test() #text:query() ) ) ) )");

//    System.err.println(out.toPrettyString());
//    System.err.println(exp.toPrettyString());

    assertEquals(out.toString(), exp.toString());
  }
}

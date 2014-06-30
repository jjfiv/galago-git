/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.Parameters;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class ProximityDFRTraversalTest {

  @Test
  public void testSomeMethod() throws Exception {
    Parameters qparams = Parameters.instance();
    qparams.set("termLambda", 0.91);

    ProximityDFRTraversal traversal = new ProximityDFRTraversal(Parameters.instance());

    Node pl2root = StructuredQuery.parse("#pdfr( test query )");
    Node out = traversal.afterNode(pl2root, qparams);
    Node exp = StructuredQuery.parse("#combine:0=0.91:1=0.08999999999999997:norm=false( #combine( "
            + "#pl2:c=6.0( #lengths:document:part=lengths() #text:test() ) "
            + "#pl2:c=6.0( #lengths:document:part=lengths() #text:query() ) ) "
            + "#combine( "
            + "#bil2:c=0.05( #lengths:document:part=lengths() #ordered:5( #text:test() #text:query() ) ) ) )");

    assertEquals(out.toString(), exp.toString());
  }
}

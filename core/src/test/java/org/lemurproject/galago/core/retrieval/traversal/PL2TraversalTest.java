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
public class PL2TraversalTest extends TestCase {
  
  public PL2TraversalTest(String testName) {
    super(testName);
  }

  public void testSomeMethod() throws Exception {
    Retrieval ret = new FakeRetrieval();
    Parameters qparams = new Parameters();

    PL2Traversal traversal = new PL2Traversal(ret, qparams);

    Node pl2root = StructuredQuery.parse("#pl2( test query )");
    Node out = traversal.afterNode( pl2root );
    Node exp = StructuredQuery.parse("#combine:norm=false( "
            + "#pl2scorer( #lengths:document:part=lengths() test ) "
            + "#pl2scorer( #lengths:document:part=lengths() query ) )");
    
    assertEquals(out.toString(), exp.toString());
  }
}

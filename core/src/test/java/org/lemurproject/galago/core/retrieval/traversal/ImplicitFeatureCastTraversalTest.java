// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor
 */
public class ImplicitFeatureCastTraversalTest {

  File indexPath;

  @Before
  public void setUp() throws IOException, IncompatibleProcessorException {
    indexPath = LocalRetrievalTest.makeIndex();
  }

  @After
  public void tearDown() throws IOException {
    FSUtil.deleteDirectory(indexPath);
  }

  // Also tests the TextFieldRewriteTraversal
  @Test
  public void testTextRewriteTraversal() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());
    LocalRetrieval retrieval = new LocalRetrieval(index, Parameters.instance());

    ImplicitFeatureCastTraversal traversal = new ImplicitFeatureCastTraversal(retrieval);
    TextFieldRewriteTraversal precedes = new TextFieldRewriteTraversal(retrieval);
    Node tree = StructuredQuery.parse("#combine( cat dog.title)");
    tree = precedes.traverse(tree, Parameters.instance()); // converts #text to #extents...
    StringBuilder transformed = new StringBuilder();
    transformed.append("#combine( ");
    transformed.append("#dirichlet( #extents:cat:part=postings() ) ");
    transformed.append("#dirichlet( #inside( #extents:dog:part=postings() ");
    transformed.append("#extents:title:part=extents() ) ) )");
    Node result = traversal.traverse(tree, Parameters.instance());
    System.err.println(transformed.toString());
    System.err.println(result.toString());
    assertEquals(transformed.toString(), result.toString());
  }

  @Test
  public void testFieldComparisonRewriteTraversal() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());
    Parameters p = Parameters.instance();
    LocalRetrieval retrieval = new LocalRetrieval(index, p);

    ImplicitFeatureCastTraversal traversal = new ImplicitFeatureCastTraversal(retrieval);
    Node tree = StructuredQuery.parse("#combine( #between( #field:title() abba zztop )");
    Node result = traversal.traverse(tree, Parameters.instance());
    assertEquals("#combine( #between:0=abba:1=zztop( #field:title() ) )", result.toString());
  }
}

// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class ImplicitFeatureCastTraversalTest extends TestCase {

  File indexPath;

  public ImplicitFeatureCastTraversalTest(String testName) {
    super(testName);
  }

  @Override
  public void setUp() throws FileNotFoundException, IOException, IncompatibleProcessorException {
    indexPath = LocalRetrievalTest.makeIndex();
  }

  @Override
  public void tearDown() throws IOException {
    Utility.deleteDirectory(indexPath);
  }

  // Also tests the TextFieldRewriteTraversal
  public void testTextRewriteTraversal() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());
    LocalRetrieval retrieval = new LocalRetrieval(index, new Parameters());

    ImplicitFeatureCastTraversal traversal = new ImplicitFeatureCastTraversal(retrieval);
    TextFieldRewriteTraversal precedes = new TextFieldRewriteTraversal(retrieval);
    Node tree = StructuredQuery.parse("#combine( cat dog.title)");
    tree = precedes.traverse(tree, new Parameters()); // converts #text to #extents...
    StringBuilder transformed = new StringBuilder();
    transformed.append("#combine( ");
    transformed.append("#dirichlet( #extents:cat:part=postings() ) ");
    transformed.append("#dirichlet( #inside( #extents:dog:part=postings() ");
    transformed.append("#extents:title:part=extents() ) ) )");
    Node result = traversal.traverse(tree, new Parameters());
    System.err.println(transformed.toString());
    System.err.println(result.toString());
    assertEquals(transformed.toString(), result.toString());
  }

  public void testFieldComparisonRewriteTraversal() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());
    Parameters p = new Parameters();
    LocalRetrieval retrieval = new LocalRetrieval(index, p);

    ImplicitFeatureCastTraversal traversal = new ImplicitFeatureCastTraversal(retrieval);
    Node tree = StructuredQuery.parse("#combine( #between( #field:title() abba zztop )");
    StringBuilder transformed = new StringBuilder();
    transformed.append("#combine( #between:0=abba:1=zztop( #field:title() ) )");
    Node result = traversal.traverse(tree, new Parameters());
    assertEquals(transformed.toString(), result.toString());
  }
}

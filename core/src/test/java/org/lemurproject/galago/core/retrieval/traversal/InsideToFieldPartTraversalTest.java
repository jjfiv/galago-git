/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.traversal.TextFieldRewriteTraversal;
import org.lemurproject.galago.core.retrieval.traversal.InsideToFieldPartTraversal;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.disk.PositionIndexReader;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class InsideToFieldPartTraversalTest extends TestCase {

  private File indexPath;

  public InsideToFieldPartTraversalTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws FileNotFoundException, IOException, IncompatibleProcessorException {
    indexPath = LocalRetrievalTest.makeIndex();
  }

  @Override
  public void tearDown() throws IOException {
    Utility.deleteDirectory(indexPath);
  }

  public void testTraversal() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());
    LocalRetrieval retrieval = new LocalRetrieval(index);
    TextFieldRewriteTraversal rewriter = new TextFieldRewriteTraversal(retrieval, new Parameters());
    Parameters inner1 = new Parameters();
    inner1.set("extents", PositionIndexReader.TermExtentIterator.class.getName());
    inner1.set("counts", PositionIndexReader.TermCountIterator.class.getName());
    
    InsideToFieldPartTraversal traversal = new InsideToFieldPartTraversal(retrieval);
    Parameters inner2 = new Parameters();
    inner2.set("extents", PositionIndexReader.TermExtentIterator.class.getName());
    inner2.set("counts", PositionIndexReader.TermCountIterator.class.getName());
    traversal.availableParts.set("field.subject", inner2);
    
    Node q1 = StructuredQuery.parse("#combine( cat dog.title donkey.subject absolute.subject)");
    Node q2 = StructuredQuery.copy(rewriter, q1); // converts #text to #extents...
    Node q3 = StructuredQuery.copy(traversal, q2); // converts #inside to #extents...

    StringBuilder transformed = new StringBuilder();

    transformed.append("#combine( ");
    transformed.append("#extents:cat:part=postings() ");
    transformed.append("#inside( #extents:dog:part=postings() ");
    transformed.append("#extents:title:part=extents() ) ");
    transformed.append("#extents:donkey:part=field.subject() ");
    transformed.append("#extents:absolute:part=field.subject() )");

    assertEquals(transformed.toString(), q3.toString());
  }
}

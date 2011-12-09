// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.traversal.BM25RelevanceFeedbackTraversal;
import org.lemurproject.galago.core.retrieval.traversal.RelevanceModelTraversal;
import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * This test is seriously a pain so all traversals
 * that make use of 2 rounds of retrieval should use the testing
 * infrastructure set up here.
 *
 * If you want to print the various statistics, uncomment some of the
 * print calls below.
 *
 * TODO: Make stronger tests to increase confidence
 *
 * @author irmarc
 */
public class RelevanceFeedbackTraversalTest extends TestCase {

  File relsFile = null;
  File queryFile = null;
  File scoresFile = null;
  File trecCorpusFile = null;
  File corpusFile = null;
  File indexFile = null;

  public RelevanceFeedbackTraversalTest(String testName) {
    super(testName);
  }

  // Build an index based on 10 short docs
  @Override
  public void setUp() throws Exception {
    File[] files = LocalRetrievalTest.make10DocIndex();
    trecCorpusFile = files[0];
    corpusFile = files[1];
    indexFile = files[2];
  }

  public void testRelevanceModelTraversal() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = new Parameters();
    p.set("index", indexFile.getAbsolutePath());
    p.set("corpus", corpusFile.getAbsolutePath());
    p.set("stemming", false);
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);
    RelevanceModelTraversal traversal = new RelevanceModelTraversal(retrieval);

    Node parsedTree = StructuredQuery.parse("#rm:fbTerms=3:fbDocs=2( #feature:dirichlet( #extents:fits:part=postings.porter() ) )");
    Node transformed = StructuredQuery.copy(traversal, parsedTree);
    // truth data
    StringBuilder correct = new StringBuilder();
    correct.append("#combine:0=0.5:1=0.5( ");
    correct.append("#combine( #feature:dirichlet( #extents:fits:part=postings.porter() ) ) ");
    correct.append("#combine:0=0.12516622340425534:1=0.041611258865248225:2=0.041611258865248225( ");
    correct.append("#feature:dirichlet( #extents:program:part=postings.porter() ) ");
    correct.append("#feature:dirichlet( #extents:shoe:part=postings.porter() ) ");
    correct.append("#feature:dirichlet( #extents:ugly:part=postings.porter() ) ) )");

    assertEquals(correct.toString(), transformed.toString());
    retrieval.close();
  }

  public void testBM25RelevanceFeedbackTraversal() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = new Parameters();
    p.set("retrievalGroup", "all");
    p.set("index", indexFile.getAbsolutePath());
    p.set("corpus", corpusFile.getAbsolutePath());
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);
    BM25RelevanceFeedbackTraversal traversal = new BM25RelevanceFeedbackTraversal(retrieval);
    Node parsedTree = StructuredQuery.parse("#bm25rf:fbDocs=3:fbTerms=2( #feature:bm25( #extents:cat:part=postings.porter() ) )");
    Node transformed = StructuredQuery.copy(traversal, parsedTree);
    //truth data
    StringBuilder correct = new StringBuilder();
    correct.append("#combine( #feature:bm25( #extents:cat:part=postings.porter() ) ");
    correct.append("#feature:bm25rf:R=3:rt=1( #extents:jumped:part=postings.porter() ) ");
    correct.append("#feature:bm25rf:R=3:rt=2( #extents:moon:part=postings.porter() ) )");
    assertEquals(correct.toString(), transformed.toString());
    
    retrieval.close();
  }

  @Override
  public void tearDown() throws Exception {
    if (relsFile != null) {
      relsFile.delete();
    }
    if (queryFile != null) {
      queryFile.delete();
    }
    if (scoresFile != null) {
      scoresFile.delete();
    }
    if (trecCorpusFile != null) {
      trecCorpusFile.delete();
    }
    if (corpusFile != null) {
      Utility.deleteDirectory(corpusFile);
    }
    if (indexFile != null) {
      Utility.deleteDirectory(indexFile);
    }
  }
}

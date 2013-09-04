// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.File;
import java.util.List;
import static junit.framework.Assert.assertEquals;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.prf.RelevanceModel1;
import org.lemurproject.galago.core.retrieval.prf.RelevanceModel3;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * This test is seriously a pain so all traversals that make use of 2 rounds of
 * retrieval should use the testing infrastructure set up here.
 *
 * If you want to print the various statistics, uncomment some of the print
 * calls below.
 *
 * TODO: Make stronger tests to increase confidence
 *
 * @author irmarc, sjh, dietz, dmf
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

  public void testRelevanceModel1Traversal() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = new Parameters();
    p.set("index", indexFile.getAbsolutePath());
    p.set("stemmedPostings", false);
    p.set("fbOrigWeight", 0.5);
    p.set("relevanceModel", RelevanceModel1.class.getName());
    p.set("rmwhitelist", "sentiwordlist.txt");
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);
    RelevanceModelTraversal traversal = new RelevanceModelTraversal(retrieval);

    Node parsedTree = StructuredQuery.parse("#rm:fbDocs=10:fbTerms=4( #dirichlet( #extents:jumped:part=postings() ) )");
    Node transformed = StructuredQuery.copy(traversal, parsedTree, new Parameters());
    // truth data
    StringBuilder correct = new StringBuilder();
    /* No sentiwordlist.txt
    correct.append("#combine:0=0.05001660577881102:1=0.05001660577881102:2=0.04165282851765748:3=0.04165282851765748( ");
    correct.append("#text:sample() ");
    correct.append("#text:ugly() ");
    correct.append("#text:cat() ");
    correct.append("#text:moon() )");
  */
    correct.append("#combine:0=0.05001660577881102:1=0.04165282851765748( ");
    correct.append("#text:ugly() ");
    correct.append("#text:moon() )");

 //  System.err.println(transformed.toString());
 //   System.err.println(correct.toString());
    
    assertEquals(correct.toString(), transformed.toString());
    
    retrieval.close();
  }

  public void testRelevanceModel3Traversal() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = new Parameters();
    p.set("index", indexFile.getAbsolutePath());
    p.set("stemmedPostings", false);
    p.set("fbOrigWeight", 0.9);
    p.set("relevanceModel", RelevanceModel3.class.getName());
    p.set("rmwhitelist", "sentiwordlist.txt");
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);
    RelevanceModelTraversal traversal = new RelevanceModelTraversal(retrieval);

    Node parsedTree = StructuredQuery.parse("#rm:fbDocs=10:fbTerms=4( #dirichlet( #extents:jumped:part=postings() ) )");
    Node transformed = StructuredQuery.copy(traversal, parsedTree, new Parameters());
    // truth data
    StringBuilder correct = new StringBuilder();

    correct.append("#combine:0=0.9:1=0.09999999999999998( #combine:fbDocs=10:fbTerms=4( #dirichlet( #extents:jumped:part=postings() ) ) ");
    correct.append("#combine:0=0.05001660577881102:1=0.04165282851765748( #text:ugly() #text:moon() ) )");
    
    //System.err.println(transformed.toString());
    //System.err.println(correct.toString());

    assertEquals(correct.toString(), transformed.toString());
 
    retrieval.close();
  }
   
  public void testRelevanceModelEmptyTraversal() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = new Parameters();
    p.set("index", indexFile.getAbsolutePath());
    p.set("stemmedPostings", false);
    p.set("fbOrigWeight", 0.9);
    p.set("relevanceModel", RelevanceModel3.class.getName());
    p.set("rmwhitelist", "sentiwordlist.txt");
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.instance(p);
    RelevanceModelTraversal traversal = new RelevanceModelTraversal(retrieval);

    Node parsedTree = StructuredQuery.parse("#rm:fbDocs=10:fbTerms=4(neverawordinedgewise)");
    Node transformed = StructuredQuery.copy(traversal, parsedTree, new Parameters());
    // truth data
    StringBuilder correct = new StringBuilder();

    correct.append("#combine:fbDocs=10:fbTerms=4( #text:neverawordinedgewise() )");
        
    //System.err.println(transformed.toString());
    //System.err.println(correct.toString());

    assertEquals(correct.toString(), transformed.toString());
    
    try {
      List <? extends ScoredDocument> results = retrieval.executeQuery(transformed).scoredDocuments;
      assertTrue(results.isEmpty());
    } catch (java.lang.IllegalArgumentException exc) {
        //Throws due to no iterator... That should get fixed.
        System.err.println(exc.getMessage());
    }
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

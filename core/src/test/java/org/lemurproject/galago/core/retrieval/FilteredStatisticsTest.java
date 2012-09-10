// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.processing.FilteredStatisticsScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.traversal.AdjustAnnotationsTraversal;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Test to make sure the FilteredStatisticsRankedModel
 * is working correctly.
 *
 * @author irmarc
 */
public class FilteredStatisticsTest extends TestCase {

  File tempPath;

  public FilteredStatisticsTest(String testName) {
    super(testName);
  }

  @Override
  public void setUp() throws Exception {
    tempPath = LocalRetrievalTest.makeIndex();
  }

  @Override
  public void tearDown() throws Exception {
    Utility.deleteDirectory(tempPath);
  }

  public void testAdjustAnnotationsTraversal() throws Exception {
    FilteredStatisticsScoringContext fssc =
            new FilteredStatisticsScoringContext();
    fssc.collectionLength = 1000;
    fssc.documentCount=100;
    fssc.tfs.put("a", 45);
    fssc.dfs.put("a", 13);
    Node root = StructuredQuery.parse(
            "#combine( #feature:dirichlet:"
            + "documentCount=1:"
            + "collectionLength=1:"
            + "collectionProbability=0.1:"
            + "nodeFrequency=1:"
            + "nodeDocumentCount=1"
            + "( #count:a() ) )");
    AdjustAnnotationsTraversal traversal = new AdjustAnnotationsTraversal(fssc);
    Node transformed = StructuredQuery.walk(traversal, root);
    
    // Check parameters
    Node featureNode = transformed.getChild(0);
    NodeParameters np = featureNode.getNodeParameters();
    assertEquals(45, (int)np.getLong("nodeFrequency"));
    assertEquals(13, (int)np.getLong("nodeDocumentCount"));
    assertEquals(1000, (int)np.getLong("collectionLength"));
    assertEquals(100, (int)np.getLong("documentCount"));
    assertEquals(((double)45)/1000, np.getDouble("collectionProbability"));
  }

  public void testFilteredStatisticsModel() throws Exception {
    Parameters globalParams = new Parameters();
    globalParams.set("processingModel", 
            "org.lemurproject.galago.core.retrieval.processing.FilteredStatisticsRankedDocumentModel");
    LocalRetrieval retrieval = new LocalRetrieval(tempPath.toString(), globalParams);
    String query = "#require( "
            + "#less( date 1/1/1900 ) "
            + "#combine("
            + " #feature:dirichlet:mu=1500( #counts:a() )"
            + " #feature:dirichlet:mu=1500( #counts:b())))";
    Node root = StructuredQuery.parse(query);
    Parameters p = new Parameters();
    p.set("requested", 5);
    root = retrieval.transformQuery(root, globalParams);
    ScoredDocument[] results = retrieval.runQuery(root, p);
    
    assertEquals(2, results.length);
    assertEquals(3, results[0].document);
    assertEquals(-0.7518724, results[0].score, 0.0001);
    assertEquals(18, results[1].document);
    assertEquals(-0.7522054, results[1].score, 0.0001);
  }
}

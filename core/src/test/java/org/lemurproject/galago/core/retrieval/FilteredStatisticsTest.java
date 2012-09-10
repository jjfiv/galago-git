// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.processing.FilteredStatisticsScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
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
            + "collectionCount=1:"
            + "collectionProbability=0.1:"
            + "nodeFrequency=1:"
            + "nodeDocumentCount=1"
            + "( #count:a() ) )");
    AdjustAnnotationsTraversal()
    Node transformed = StructuredQuery.walk(null, root)
  }

  public void testFilteredStatisticsModel() throws Exception {

  }
}

// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.scoring;

import org.lemurproject.galago.core.scoring.BM25RFScorer;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.extents.FakeExtentIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
public class BM25RFScorerTest extends TestCase {

  int[][] dummy = {};
  int[][] five = {
    {1, 2, 3},
    {5, 10, 60},
    {1, 90},
    {4, 78, 2343},
    {100}};
  FakeExtentIterator iterator;

  public void testScorer() throws Exception {
    iterator = new FakeExtentIterator(dummy);

    // test empty
    NodeParameters parameters = new NodeParameters();
    parameters.set("documentCount", 0);
    BM25RFScorer scorer = new BM25RFScorer(new Parameters(), parameters, iterator);
    assertEquals(0.0, scorer.score(1, 1));
    assertEquals(0.0, scorer.score(200, 957));

    // set some values
    iterator = new FakeExtentIterator(five);
    parameters.set("rt", 3);
    parameters.set("R", 10);
    parameters.set("documentCount", 1000);
    scorer = new BM25RFScorer(new Parameters(), parameters, iterator);
    assertEquals(1.72186, scorer.score(1, 1), 0.001);
    assertEquals(1.72186, scorer.score(1, 234565), 0.001);

    // Fill in ft
    parameters.set("ft", 5);
    scorer = new BM25RFScorer(new Parameters(), parameters, null);
    assertEquals(1.72186, scorer.score(1, 1), 0.001);
    assertEquals(1.72186, scorer.score(9, 9), 0.001);
  }
}

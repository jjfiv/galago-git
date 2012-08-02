/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.File;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.LengthsReader.LengthsIterator;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DirichletSmoothingIteratorTest extends TestCase {

  public DirichletSmoothingIteratorTest(String testName) {
    super(testName);
  }

  public void testSmoothing() throws Exception {
    File trec = Utility.createTemporary();
    File idx = Utility.createTemporaryDirectory();
    try {
      StringBuilder data = new StringBuilder();
      for (int i = 0; i < 100; i++) {
        data.append(AppTest.trecDocument("d-" + i, "example trec document sample one " + i));
      }
      Utility.copyStringToFile(data.toString(), trec);
      App.run(new String[]{"build",
                "--indexPath=" + idx.getAbsolutePath(),
                "--inputPath+" + trec.getAbsolutePath()});

      Parameters empty = new Parameters();

      LocalRetrieval ret = new LocalRetrieval(idx.getAbsolutePath(), empty);

      ScoringContext sc = new ScoringContext();
      Node dirNode1 = StructuredQuery.parse("#feature:dirichlet( one )");
      dirNode1 = ret.transformQuery(dirNode1, empty);

      Node dirNode2 = StructuredQuery.parse("#dirichlet( #lengths:part=lengths() one )");
      dirNode2 = ret.transformQuery(dirNode2, empty);

      MovableScoreIterator dir1 = (MovableScoreIterator) ret.createIterator(empty, dirNode1, sc);
      MovableScoreIterator dir2 = (MovableScoreIterator) ret.createIterator(empty, dirNode2, sc);

      Node lens = StructuredQuery.parse("#lengths:part=lengths()");
      sc.addLength("", (LengthsIterator) ret.createIterator(empty, lens, sc));

      while (!dir1.isDone() || !dir2.isDone()) {
        assertEquals(dir1.currentCandidate(), dir2.currentCandidate());
        int d = dir1.currentCandidate();
        sc.document = d;
        sc.moveLengths(d);

        dir1.moveTo(d);
        dir2.moveTo(d);

        assert (dir1.hasMatch(d));
        assert (dir2.hasMatch(d));

//        System.err.println(dir1.getAnnotatedNode().toString());
//        System.err.println(dir2.getAnnotatedNode().toString());

        assertEquals(dir1.score(), dir2.score());

        dir1.movePast(d);
        dir2.movePast(d);
      }


    } finally {
      if (idx != null) {
        Utility.deleteDirectory(idx);
      }
      if (trec != null) {
        trec.delete();
      }
    }
  }
}

/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import java.io.File;
import java.util.*;
import junit.framework.TestCase;
import org.lemurproject.galago.contrib.util.TestingUtils;
import org.lemurproject.galago.core.index.mem.MemorySparseDoubleIndex;
import org.lemurproject.galago.core.retrieval.CachedRetrieval;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class LearnerTest extends TestCase {

  public LearnerTest(String name) {
    super(name);
  }

  public void testLearnerCaching() throws Exception {
    File index = null;
    File qrels = null;

    try {
      File[] files = TestingUtils.make10DocIndex();
      files[0].delete(); // trecCorpus not required
      Utility.deleteDirectory(files[1]); // corpus not required
      index = files[2]; // index is required
      qrels = Utility.createTemporary();

      Retrieval ret = RetrievalFactory.instance(index.getAbsolutePath(), Parameters.parse("{\"cache\" : true, \"flattenCombine\" : false, \"cacheScores\": true}"));

      String qrelData =
              "q1 x 2 1\n"
              + "q1 x 5 1\n"
              + "q1 x 8 1\n"
              + "q2 x 3 1\n"
              + "q2 x 7 1\n";
      Utility.copyStringToFile(qrelData, qrels);

      // init learn params with queries
      Parameters learnParams = Parameters.parse("{\"queries\": [{\"number\":\"q1\",\"text\":\"#sdm( jump moon )\"}, {\"number\":\"q2\",\"text\":\"#sdm( everything shoe )\"}]}");
      learnParams.set("learner", "grid");
      learnParams.set("qrels", qrels.getAbsolutePath());
      // add two parameters
      List<Parameters> learnableParams = new ArrayList();
      learnableParams.add(Parameters.parse("{\"name\":\"uniw\", \"max\":1.0, \"min\":-1.0}"));
      learnableParams.add(Parameters.parse("{\"name\":\"odw\", \"max\":1.0, \"min\":-1.0}"));
      learnableParams.add(Parameters.parse("{\"name\":\"uww\", \"max\":1.0, \"min\":-1.0}"));
      learnParams.set("learnableParameters", learnableParams);
      // add sum rule to ensure sums to 1
      Parameters normalRule = new Parameters();
      normalRule.set("mode", "sum");
      normalRule.set("params", Arrays.asList(new String[]{"0", "1"}));
      normalRule.set("value", 1D);
      learnParams.set("normalization", new ArrayList());
      learnParams.getList("normalization").add(normalRule);

      learnParams.set("gridSize", 3);
      learnParams.set("restarts", 1);

      Learner learner = LearnerFactory.instance(learnParams, ret);

      // List<Parameters> params = learner.learn();
      // for (Parameters p : params) {
      //  System.err.println(p);
      // }

      LocalRetrieval r = (LocalRetrieval) learner.retrieval;

      // generate some new random parameters
      RetrievalModelInstance rnd = learner.generateRandomInitalValues();
      Parameters settings = rnd.toParameters();

      for (String number : learner.queries.getQueryNumbers()) {
        Node root = learner.queries.getNode(number).clone();
        r.getGlobalParameters().copyFrom(settings);
        root = learner.ensureSettings(root, settings);
        root = r.transformQuery(root, settings);

        // check which nodes have been cached
        
        // node is an SDM - root and direct children are not cached - all others are cached
        MovableIterator i = (MovableIterator) r.createIterator(new Parameters(), root, new ScoringContext());
        assertFalse(i instanceof MemorySparseDoubleIndex.ScoresIterator);
        for (Node child : root.getInternalNodes()) {
          i = (MovableIterator) r.createIterator(new Parameters(), child, new ScoringContext());
          assertFalse(i instanceof MemorySparseDoubleIndex.ScoresIterator);
          for (Node subchild : child.getInternalNodes()) {
            i = (MovableIterator) r.createIterator(new Parameters(), subchild, new ScoringContext());
            assertTrue(i instanceof MemorySparseDoubleIndex.ScoresIterator);
          }
        }
      }


    } finally {
      if (index != null) {
        Utility.deleteDirectory(index);
      }
      if (qrels != null) {
        qrels.delete();
      }
    }
  }
}

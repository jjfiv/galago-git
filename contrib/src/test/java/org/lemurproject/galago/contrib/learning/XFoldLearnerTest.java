/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;
import org.lemurproject.galago.contrib.util.TestingUtils;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class XFoldLearnerTest extends TestCase {

  public XFoldLearnerTest(String name) {
    super(name);
  }

  public void testCoordSearch() throws Exception {
    File index = null;
    File qrels = null;

    try {
      File[] files = TestingUtils.make10DocIndex();
      files[0].delete(); // trecCorpus not required
      Utility.deleteDirectory(files[1]); // corpus not required
      index = files[2]; // index is required
      qrels = Utility.createTemporary();

      Retrieval ret = RetrievalFactory.instance(index.getAbsolutePath(), new Parameters());

      String qrelData =
              "q1 x 2 1\n"
              + "q1 x 5 1\n"
              + "q1 x 8 1\n"
              + "q2 x 3 1\n"
              + "q2 x 7 1\n";
      Utility.copyStringToFile(qrelData, qrels);

      // init learn params with queries
      Parameters learnParams = Parameters.parse("{\"queries\": [{\"number\":\"q1\",\"text\":\"#combine:0=0.5:1=0.5( jump moon )\"}, {\"number\":\"q2\",\"text\":\"#combine:0=0.5:1=0.5( everything shoe )\"}]}");
      learnParams.set("learner", "xfold");
      learnParams.set("xfolds", 2);
      learnParams.set("xfoldLearner", "coord");
      
      learnParams.set("qrels", qrels.getAbsolutePath());
      // add two parameters
      List<Parameters> learnableParams = new ArrayList();
      learnableParams.add(Parameters.parse("{\"name\":\"0\"}"));
      learnableParams.add(Parameters.parse("{\"name\":\"1\"}"));
      learnParams.set("learnableParameters", learnableParams);
      // add sum rule to ensure sums to 1
      Parameters normalRule = new Parameters();
      normalRule.set("mode", "sum");
      normalRule.set("params", Arrays.asList(new String[]{"0", "1"}));
      normalRule.set("value", 1.0);
      learnParams.set("normalization", new ArrayList());
      learnParams.getList("normalization").add(normalRule);
      

      learnParams.set("restarts", 1);
      learnParams.set("initialParameters", new ArrayList());
      learnParams.getList("initialParameters").add(Parameters.parse("{\"0\":0.9,\"1\":-0.2}"));
      
      Learner learner = LearnerFactory.instance(learnParams, ret);
      List<RetrievalModelInstance> res = learner.learn();

      for (RetrievalModelInstance r : res) {
        System.err.println(r.toParameters().toString());
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

/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class XFoldLearner extends Learner {

  int xfoldCount;
  List<Learner> foldLearners;
  Map<Integer, List<Parameters>> queryFolds;

  public XFoldLearner(Parameters p, Retrieval r) throws Exception {
    super(p, r);

    // required parameters:
    assert (p.isLong("xfolds")) : this.getClass().getName() + " requires `xfolds' parameter, of type long";
    assert (!p.containsKey("xfoldLearner")
            || p.isString("xfoldLearner")) : this.getClass().getName() + " requires `xfoldLeaner' parameter, of type String";

    // create one learner for each xfold.
    xfoldCount = (int) p.getLong("xfolds");
    foldLearners = new ArrayList(xfoldCount);
    queryFolds = new HashMap(xfoldCount);

    // randomize order of queries
    List<Parameters> queriesCopy = new ArrayList(this.queries);
    Collections.shuffle(queriesCopy, random);

    // split queries into folds
    int foldSize = (int) Math.ceil((double) queriesCopy.size() / (double) xfoldCount);
    for (int foldId = 0; foldId < xfoldCount; foldId++) {
      List<Parameters> foldQueries = queriesCopy.subList(foldId * foldSize, (foldId + 1) * foldSize);
      // create new learner for each fold
      Parameters copy = p.clone();
      copy.set("learner", p.get("xfoldLearner", "default")); // overwrite //
      copy.remove("query");
      copy.set("queries", foldQueries); // overwrite //
      queryFolds.put(foldId, foldQueries);
      foldLearners.add(LearnerFactory.instance(copy, retrieval));
    }
  }

  @Override
  public LearnableParameterInstance learn(LearnableParameterInstance initialSettings) throws Exception {
    // for each fold return learnt parameters
    List<LearnableParameterInstance> learntParams = new ArrayList();
    for (Learner fl : foldLearners) {
      LearnableParameterInstance result = fl.learn(initialSettings);
      learntParams.add(result);
    }

    return LearnableParameterInstance.average(learntParams);
  }
}

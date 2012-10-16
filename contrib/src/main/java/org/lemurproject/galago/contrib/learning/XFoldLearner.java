/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

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
  Map<Integer, List<String>> queryFolds;

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
    List<String> queryNumbersCopy = new ArrayList(this.queries.queryNumbers);
    Collections.shuffle(queryNumbersCopy, random);

    // split queries into folds
    int foldSize = (int) Math.ceil((double) queryNumbersCopy.size() / (double) xfoldCount);
    for (int foldId = 0; foldId < xfoldCount; foldId++) {
      List<String> xfoldQueryNumbers = queryNumbersCopy.subList(foldId * foldSize, (foldId + 1) * foldSize);
      // create new learner for each fold
      Parameters copy = p.clone();
      copy.set("learner", p.get("xfoldLearner", "default")); // overwrite //
      copy.remove("query");
      copy.set("queries", queries.getParametersSubset(xfoldQueryNumbers)); // overwrite //
      queryFolds.put(foldId, xfoldQueryNumbers);
      foldLearners.add(LearnerFactory.instance(copy, retrieval));
    }
  }

  @Override
  public RetrievalModelInstance learn(RetrievalModelInstance initialSettings) throws Exception {
    // for each fold return learnt parameters
    List<RetrievalModelInstance> learntParams = new ArrayList();
    for (Learner fl : foldLearners) {
      RetrievalModelInstance result = fl.learn(initialSettings);
      learntParams.add(result);
    }

    return RetrievalModelInstance.average(learntParams);
  }
}

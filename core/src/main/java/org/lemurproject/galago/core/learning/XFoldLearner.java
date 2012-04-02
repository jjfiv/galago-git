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

  List<Learner> foldLearners;
  Map<Integer, List<Parameters>> queryFolds;

  public XFoldLearner(Parameters p, Retrieval r) throws Exception {
    super(p, r);

    // split up queries into folds
    // open a new learner for each fold
    foldLearners = new ArrayList();
    queryFolds = new HashMap();

    List<Parameters> queriesCopy = new ArrayList(this.queries);
    Collections.shuffle(queriesCopy, random);
    int foldSize = (int) Math.ceil((double) queriesCopy.size() / (double) p.getLong("xfolds"));
    for (int fid = 0; fid < p.getLong("xfolds"); fid++) {
      List<Parameters> foldQueries = queriesCopy.subList(fid * foldSize, (fid + 1) * foldSize);
      Parameters copy = p.clone();
      copy.remove("query");
      copy.set("queries", foldQueries);
      queryFolds.put(fid, foldQueries);
      foldLearners.add(LearnerFactory.instance(copy, retrieval));
    }
  }

  @Override
  public Parameters learn(Parameters initialSettings) throws Exception {
    // for each fold return learnt parameters
    return initialSettings;
  }


  
}

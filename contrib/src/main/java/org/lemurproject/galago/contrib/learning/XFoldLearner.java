/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import org.lemurproject.galago.core.eval.QuerySetResults;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class XFoldLearner extends Learner {
  
  private int xfoldCount;
  private Map<Integer, Learner> foldLearners;
  private Map<Integer, List<String>> trainQueryFolds;
  private Map<Integer, List<String>> testQueryFolds;
  
  public XFoldLearner(Parameters p, Retrieval r) throws Exception {
    super(p, r);

    // required parameters:
    assert (p.isLong("xfolds")) : this.getClass().getName() + " requires `xfolds' parameter, of type long";
    assert (!p.containsKey("xfoldLearner")
            || p.isString("xfoldLearner")) : this.getClass().getName() + " requires `xfoldLeaner' parameter, of type String";

    // create one learner for each xfold.
    xfoldCount = (int) p.getLong("xfolds");
    foldLearners = new HashMap(xfoldCount);
    trainQueryFolds = new HashMap(xfoldCount);
    testQueryFolds = new HashMap(xfoldCount);

    // randomize order of queries
    List<String> queryNumbersCopy = new ArrayList(this.queries.queryNumbers);
    Collections.shuffle(queryNumbersCopy, random);

    // split queries into folds
    int foldSize = (int) Math.ceil((double) queryNumbersCopy.size() / (double) xfoldCount);
    for (int foldId = 0; foldId < xfoldCount; foldId++) {
      List<String> xfoldQueryNumbers = queryNumbersCopy.subList(foldId * foldSize, (foldId + 1) * foldSize);
      List<String> xfoldQueryNumbersInverse = new ArrayList(queryNumbersCopy);
      xfoldQueryNumbersInverse.removeAll(xfoldQueryNumbers);
      
      logger.log(Level.INFO, "Fold: {0} contains {1} + {2} = {3} queries", new Object[]{foldId, xfoldQueryNumbers.size(), xfoldQueryNumbersInverse.size(), this.queries.queryNumbers.size()});
      
      testQueryFolds.put(foldId, xfoldQueryNumbers);
      trainQueryFolds.put(foldId, xfoldQueryNumbersInverse);

      // create new learner for each fold
      // use the train queries for the fold
      Parameters copy = p.clone();
      copy.set("learner", p.get("xfoldLearner", "default")); // overwrite //
      copy.remove("query");
      copy.set("queries", queries.getParametersSubset(xfoldQueryNumbersInverse)); // overwrite //
      foldLearners.put(foldId, LearnerFactory.instance(copy, retrieval));
    }
  }

  /**
   * learning function - returns a list of learnt parameters
   */
  @Override
  public List<Parameters> learn() throws Exception {
    List<Parameters> learntParams = new ArrayList();
    // one set of results per fold.
    for (int fid : foldLearners.keySet()) {
      // one result per repeat
      for (int i = 0; i < this.initialSettings.size(); i++) {
        RetrievalModelInstance s = learn(fid, this.initialSettings.get(i).clone());

        // annotate with scores using test Queries
        Parameters p = new Parameters();
        p.set("foldId", fid);
        p.set("repeatId", i);
        p.set("testScore", this.evaluateSpecificQueries(fid, s, this.testQueryFolds.get(fid)));
        p.set("trainScore", this.evaluateSpecificQueries(fid, s, this.trainQueryFolds.get(fid)));
        p.set("score", this.evaluateSpecificQueries(fid, s, new ArrayList(this.queries.queryNumbers)));
        p.set("learntSettings", s.toParameters());
        learntParams.add(p);
        
        System.err.println(p.toPrettyString());
      }
    }
    return learntParams;
  }
  
  public RetrievalModelInstance learn(int fid, RetrievalModelInstance initialSettings) throws Exception {
    RetrievalModelInstance result = foldLearners.get(fid).learn(initialSettings);
    return result;
  }
  
  @Override
  public RetrievalModelInstance learn(RetrievalModelInstance initialSettings) throws Exception {
    throw new RuntimeException("Function not availiable for xfold learner.");
  }
  
  protected double evaluateSpecificQueries(int foldId, RetrievalModelInstance instance, List<String> qids) throws Exception {
    long start = 0;
    long end = 0;
    
    HashMap<String, ScoredDocument[]> resMap = new HashMap();

    // ensure the global parameters contain the current settings.
    Parameters settings = instance.toParameters();
    this.retrieval.getGlobalParameters().copyFrom(settings);
    
    for (String number : qids) {
      
      Node root = this.queries.getNode(number).clone();
      root = this.ensureSettings(root, settings);
      root = this.retrieval.transformQuery(root, settings);

      //  need to add queryProcessing params some extra stuff to 'settings'
      start = System.currentTimeMillis();
      ScoredDocument[] scoredDocs = this.retrieval.runQuery(root, settings);
      end = System.currentTimeMillis();
      
      if (scoredDocs != null) {
        resMap.put(number, scoredDocs);
      }
    }
    
    QuerySetResults results = new QuerySetResults(resMap);
    results.ensureQuerySet(queries.getParametersSubset(this.testQueryFolds.get(foldId)));
    double r = evalFunction.evaluate(results, qrels);
    
    logger.info("Specific-query-set run time: " + (end - start) + ", settings : " + settings.toString() + ", score : " + r);
    
    return r;
  }
}

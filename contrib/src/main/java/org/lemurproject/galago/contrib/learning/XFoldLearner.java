/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
  private Map<Integer, Parameters> foldParameters;
  private Map<Integer, Learner> foldLearners;
  private Map<Integer, List<String>> trainQueryFolds;
  private Map<Integer, List<String>> testQueryFolds;
  final private ArrayList queryNumbers;

  public XFoldLearner(Parameters p, Retrieval r) throws Exception {
    super(p, r);

    // required parameters:
    assert (p.isLong("xfolds")) : this.getClass().getName() + " requires `xfolds' parameter, of type long";
    assert (!p.containsKey("xfoldLearner")
            || p.isString("xfoldLearner")) : this.getClass().getName() + " requires `xfoldLeaner' parameter, of type String";

    // create one set of parameters (and learner) for each xfold.
    xfoldCount = (int) p.getLong("xfolds");
    trainQueryFolds = new HashMap(xfoldCount);
    testQueryFolds = new HashMap(xfoldCount);
    foldParameters = new HashMap(xfoldCount);
    foldLearners = new HashMap(xfoldCount);

    // randomize order of queries
    queryNumbers = new ArrayList(this.queries.queryIdentifiers);
    Collections.shuffle(queryNumbers, random);

    // split queries into folds
    int foldSize = (int) Math.ceil((double) queryNumbers.size() / (double) xfoldCount);
    for (int foldId = 0; foldId < xfoldCount; foldId++) {
      List<String> xfoldQueryNumbers = queryNumbers.subList(foldId * foldSize, (foldId + 1) * foldSize);
      List<String> xfoldQueryNumbersInverse = new ArrayList(queryNumbers);
      xfoldQueryNumbersInverse.removeAll(xfoldQueryNumbers);

      logger.info(String.format("Fold: %d contains %d + %d = %d queries", foldId, xfoldQueryNumbers.size(), xfoldQueryNumbersInverse.size(), this.queries.queryIdentifiers.size()));

      testQueryFolds.put(foldId, xfoldQueryNumbers);
      trainQueryFolds.put(foldId, xfoldQueryNumbersInverse);

      // create new learner for each fold
      // use the train queries for the fold
      Parameters copy = p.clone();
      copy.set("name", name + "-foldId-" + foldId);
      copy.set("learner", p.get("xfoldLearner", "default")); // overwrite //
      copy.remove("query");
      copy.remove("queries");
      copy.set("queries", queries.getParametersSubset(xfoldQueryNumbersInverse)); // overwrite //
      foldParameters.put(foldId, copy);
      foldLearners.put(foldId, LearnerFactory.instance(copy, retrieval));
    }
  }

  /**
   * learning function - returns a list of learnt parameters
   */
  @Override
  public List<RetrievalModelInstance> learn() throws Exception {
    final List<RetrievalModelInstance> learntParams = new ArrayList();

//    if (threading) {
//      ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
//      final List<Exception> exceptions = Collections.synchronizedList(new ArrayList());
//      final CountDownLatch latch = new CountDownLatch(foldLearners.size() * restarts);
//
//      // one set of results per fold.
//      for (int fid : foldLearners.keySet()) {
//        final int foldId = fid;
//        final Learner foldLearner = foldLearners.get(foldId);
//        Thread t = new Thread() {
//
//          @Override
//          public void run() {
//            try {
//              List<RetrievalModelInstance> results = foldLearner.learn();
//              for (RetrievalModelInstance res : results) {
//                double testScore = evaluateSpecificQueries(foldId, res, testQueryFolds.get(foldId));
//                res.setAnnotation("testScore", Double.toString(testScore));
//                double allScore = evaluateSpecificQueries(foldId, res, queryNumbers);
//                res.setAnnotation("allScore", Double.toString(allScore));
//              }
//              learntParams.addAll(results);
//            } catch (Exception e) {
//              exceptions.add(e);
//            } finally {
//              latch.countDown();
//            }
//          }
//        };
//        threadPool.execute(t);
//      }
//
//      while (latch.getCount() > 0) {
//        try {
//          latch.await();
//        } catch (InterruptedException e) {
//          // do nothing
//        }
//      }
//
//      threadPool.shutdown();
//
//      if (!exceptions.isEmpty()) {
//        for (Exception e : exceptions) {
//          System.err.println("Caught exception: \n" + e.toString());
//          e.printStackTrace();
//        }
//      }
//
//    } else {
      // one set of results per fold.
      for (int foldId : foldLearners.keySet()) {
        List<RetrievalModelInstance> results = foldLearners.get(foldId).learn();
        for (RetrievalModelInstance res : results) {
          double testScore = evaluateSpecificQueries(foldId, res, testQueryFolds.get(foldId));
          res.setAnnotation("testScore", Double.toString(testScore));
          double allScore = evaluateSpecificQueries(foldId, res, queryNumbers);
          res.setAnnotation("allScore", Double.toString(allScore));
        }
        learntParams.addAll(results);
      }
//    }
    return learntParams;
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

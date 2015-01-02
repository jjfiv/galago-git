/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import com.google.common.collect.Lists;
import org.lemurproject.galago.core.eval.EvalDoc;
import org.lemurproject.galago.core.eval.QuerySetResults;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.util.*;

/**
 *
 * @author sjh
 */
public class XFoldLearner extends Learner {

  private int xfoldCount;
  private Map<Integer, Parameters> foldTrainParameters;
  private Map<Integer, Parameters> foldTestParameters;
  private Map<Integer, Learner> foldLearners;
  private Map<Integer, List<String>> trainQueryFolds;
  private Map<Integer, List<String>> testQueryFolds;
  private ArrayList<String> queryNumbers;
  private boolean execute;

  public XFoldLearner(Parameters p, Retrieval r) throws Exception {
    super(p, r);

    // required parameters:
    assert (p.isLong("xfolds")) : this.getClass().getName() + " requires `xfolds' parameter, of type long";
    assert (!p.containsKey("xfoldLearner")
            || p.isString("xfoldLearner")) : this.getClass().getName() + " requires `xfoldLeaner' parameter, of type String";

    execute = p.get("execute", true);

    // create one set of parameters (and learner) for each xfold.
    xfoldCount = (int) p.getLong("xfolds");
    trainQueryFolds = new HashMap<>(xfoldCount);
    testQueryFolds = new HashMap<>(xfoldCount);
    foldTrainParameters = new HashMap<>(xfoldCount);
    foldTestParameters = new HashMap<>(xfoldCount);
    foldLearners = new HashMap<>(xfoldCount);

    // randomize order of queries
    queryNumbers = new ArrayList<String>(this.queries.queryIdentifiers);
    Collections.shuffle(queryNumbers, random);

    // split queries into folds
    int foldSize = (int) Math.ceil((double) queryNumbers.size() / (double) xfoldCount);
    for (int foldId = 0; foldId < xfoldCount; foldId++) {
      List<String> xfoldQueryNumbers = queryNumbers.subList(foldId * foldSize, (foldId + 1) * foldSize);
      List<String> xfoldQueryNumbersInverse = new ArrayList<String>(queryNumbers);
      xfoldQueryNumbersInverse.removeAll(xfoldQueryNumbers);

      outputTraceStream.println(String.format("%s - Fold: %d contains %d + %d = %d queries", name, foldId, xfoldQueryNumbers.size(), xfoldQueryNumbersInverse.size(), this.queries.queryIdentifiers.size()));

      testQueryFolds.put(foldId, xfoldQueryNumbers);
      trainQueryFolds.put(foldId, xfoldQueryNumbersInverse);

      // create new learner for each fold
      Parameters copy = p.clone();

      if (outputFolder != null) {
        // write the test queries for the fold to a file (allows random-testing of test-fold)
        copy.set("name", name + "-foldId-" + foldId);
        copy.set("learner", p.get("xfoldLearner", "default")); // overwrite //
        copy.remove("query");
        copy.remove("queries");
        copy.set("queries", queries.getParametersSubset(xfoldQueryNumbers)); // overwrite //
        Utility.copyStringToFile(copy.toPrettyString(), new File(outputFolder, name + "-test-fold-" + foldId + ".json"));
      }

      // write the train queries for the fold
      copy.set("name", name + "-foldId-" + foldId);
      copy.set("learner", p.get("xfoldLearner", "default")); // overwrite //
      copy.remove("queries");
      copy.set("queries", queries.getParametersSubset(xfoldQueryNumbersInverse)); // overwrite //
      foldTrainParameters.put(foldId, copy);

      if (outputFolder != null) {
        Utility.copyStringToFile(copy.toPrettyString(), new File(outputFolder, name + "-train-fold-" + foldId + ".json"));
      }

      foldLearners.put(foldId, LearnerFactory.instance(copy, retrieval));
    }

    // copy each one of these to a file .fold1, .fold2, ...
  }

  public void close() {
    super.close();
    for (Learner l : this.foldLearners.values()) {
      l.close();
    }
  }

  /**
   * learning function - returns a list of learnt parameters
   */
  @Override
  public RetrievalModelInstance learn() throws Exception {
    if (execute) {
      final List<RetrievalModelInstance> learntParams = new ArrayList<RetrievalModelInstance>();

      // one set of results per fold.
      for (int foldId : foldLearners.keySet()) {
        RetrievalModelInstance result = foldLearners.get(foldId).learn();
        double testScore = evaluateSpecificQueries(result, testQueryFolds.get(foldId));
        result.setAnnotation("testScore", Double.toString(testScore));
        double allScore = evaluateSpecificQueries(result, queryNumbers);
        result.setAnnotation("allScore", Double.toString(allScore));

        this.outputPrintStream.println(result.toPrettyString());

        learntParams.add(result);
      }

      // take an average value across fold instances
      Parameters settings = Parameters.create();
      for (String param : this.learnableParameters.getParams()) {
        double setting = 0.0;
        for (RetrievalModelInstance foldOpt : learntParams) {
          setting += foldOpt.get(param);
        }
        setting /= learntParams.size();
        settings.set(param, setting);
      }
      RetrievalModelInstance averageParams = new RetrievalModelInstance(this.learnableParameters, settings);
      double score = evaluateSpecificQueries(averageParams, queryNumbers);
      averageParams.setAnnotation("score", Double.toString(score));
      averageParams.setAnnotation("name", name + "-xfold-avg");

      outputPrintStream.println(averageParams.toPrettyString());

      return averageParams;
    } else {
      outputPrintStream.println("NOT OPTIMIZING, returning random parameters.");
      return this.generateRandomInitalValues();
    }
  }

  protected double evaluateSpecificQueries(RetrievalModelInstance instance, List<String> qids) throws Exception {
    long start = 0;
    long end = 0;

    HashMap<String, List<EvalDoc>> resMap = new HashMap<>();

    // ensure the global parameters contain the current settings.
    Parameters settings = instance.toParameters();
    this.retrieval.getGlobalParameters().copyFrom(settings);

    for (String number : qids) {

      Node root = this.queries.getNode(number).clone();
      root = this.ensureSettings(root, settings);
      root = this.retrieval.transformQuery(root, settings);

      //  need to add queryProcessing params some extra stuff to 'settings'
      start = System.currentTimeMillis();
      List<? extends EvalDoc> scoredDocs = this.retrieval.executeQuery(root, settings).scoredDocuments;
      end = System.currentTimeMillis();

      if (scoredDocs != null) {
        resMap.put(number, Lists.newArrayList(scoredDocs));
      }
    }

    QuerySetResults results = new QuerySetResults(resMap);
    results.ensureQuerySet(queries.getParametersSubset(qids));
    double r = evalFunction.evaluate(results, qrels);

    outputTraceStream.println("Specific-query-set run time: " + (end - start) + ", settings : " + settings.toString() + ", score : " + r);

    return r;
  }
}

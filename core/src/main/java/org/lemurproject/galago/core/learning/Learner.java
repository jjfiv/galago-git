/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import org.lemurproject.galago.core.eval.QuerySetJudgments;
import org.lemurproject.galago.core.eval.QuerySetResults;
import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluator;
import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluatorFactory;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.BatchSearch;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Parameters.Type;

/**
 * Main interface for learning methods - eg. coord-ascent, gradient ascent, etc.
 *
 * Required parameters: 1. index -> see retrieval factory for possible options
 * 2. queries -> see batch-search 3. qrels -> see eval 4. learnableParameters ->
 * e.g. [ { "name" : "mu", "max" : 10000, "min" : 1, "isInteger": true}, <- this
 * parameter is optional. -> ... ]
 *
 * Optional Parameters: 1. metric [default = map] -> see eval
 *
 *
 * TODO LIST:
 *
 * 2. randomizeParameterValues [default = true] 2.1 randomRestarts [default = 5]
 * : this value is used when randomizeParameterValues == true 2.2
 * initialParameters : this value is required if randomizeParameterValues ==
 * false e.g [{ "mu" : 1500, ... }, { "mu" : 1000, ...}, ...]
 *
 * 3. x-fold cross validation [default = 3] // - perhaps a meta learner - x
 * learning children each with a subset of queries
 *
 * @author sjh
 */
public abstract class Learner {

  protected final Logger logger;
  protected final Random random;
  protected final Retrieval retrieval;
  // variable parameters
  protected List<Parameters> queries;
  protected Map<String, Node> queryRoots;
  protected QuerySetJudgments qrels;
  protected QuerySetEvaluator evalFunction;
  protected Set<String> learnableParameters;
  protected Map<String, Double> learnableParametersMax;
  protected Map<String, Double> learnableParametersMin;
  protected Map<String, Double> learnableParametersRange;
  // optimized parameters - mapping is from index of initial settings to optimal parameters
  protected List<Parameters> initialSettings;
  protected Map<Integer, Double> optimizedParameterScores;
  protected Map<Integer, Parameters> optimizedParameters;
  // evaluation cache to avoid recalculating scores for known settings
  protected Map<String, Double> testedParameters;

  public Learner(Parameters p, Retrieval r) throws Exception {
    logger = Logger.getLogger(this.getClass().getName());
    retrieval = r;
    random = (p.isLong("rndInit"))? new Random(p.getLong("rndInit")) : new Random() ;

    initialize(p);
  }

  /**
   * learning function
   *  - returns a list of learnt parameters
   */
  public List<Parameters> learn() throws Exception {
    for (int i = 0; i < this.initialSettings.size(); i++) {
      Parameters s = learn(this.initialSettings.get(i).clone());
      optimizedParameters.put(i, s);
      optimizedParameterScores.put(i, this.evaluate(s));
    }
    return new ArrayList(optimizedParameters.values());
  }

  /**
   * instance learning function - should return the best parameters discovered for these initial settings.
   */
  public abstract Parameters learn(Parameters initialSettings) throws Exception;

  /**
   * Getters and Setters
   *  - currently only for the initial settings
   */
  public List<Parameters> getInitialSettings() {
    return this.initialSettings;
  }

  public void setInitialSettings(List<Parameters> initialSettings) {
    this.initialSettings = initialSettings;
  }

  /**
   * UTILITY FUNCTIONS : functions that can be used inside of any implemented learner
   */
  protected void initialize(Parameters p) throws IOException {
    queries = BatchSearch.collectQueries(p);
    qrels = new QuerySetJudgments(p.getString("qrels"));
    evalFunction = QuerySetEvaluatorFactory.instance(p.get("metric", "map"));

    learnableParameters = new HashSet();
    learnableParametersMax = new HashMap();
    learnableParametersMin = new HashMap();
    learnableParametersRange = new HashMap();

    for (Parameters toLearn : (List<Parameters>) p.getList("learnableParameters")) {
      String param = toLearn.getString("name");
      double max = toLearn.getDouble("max");
      double min = toLearn.getDouble("min");
      assert max > min : "Parameters " + param + " min is greater than max.";
      learnableParameters.add(param);
      learnableParametersMax.put(param, max);
      learnableParametersMin.put(param, min);
      learnableParametersRange.put(param, max - min);
    }

    testedParameters = new HashMap();
    optimizedParameters = new HashMap();
    optimizedParameterScores = new HashMap();

    long restarts = p.get("restarts", 3);
    if (p.isList("initialParameters", Type.MAP)) {
      initialSettings = (List<Parameters>) p.getList("initialParameters");
    } else {
      initialSettings = new ArrayList();
    }
    while (initialSettings.size() < restarts) {
      initialSettings.add(generateRandomInitalValues());
    }

    // we only want to parse queries once.
    queryRoots = new HashMap();
    for (Parameters query : this.queries) {
      String number = query.getString("number");
      String text = query.getString("text");
      Node root = StructuredQuery.parse(text);
      queryRoots.put(number, root);
    }
  }

  /**
   * generateRandomInitalValues
   */
  protected Parameters generateRandomInitalValues() {
    Parameters init = new Parameters();
    for (String p : this.learnableParameters) {
      double val = random.nextDouble();
      val *= this.learnableParametersRange.get(p);
      val += this.learnableParametersMin.get(p);
      init.set(p, val);
    }
    return init;
  }

  /**
   * Applies new parameter settings to each query, runs the query, evaluates the
   * scored document results
   *
   * Runs all of the queries with the new parameter settings
   *
   */
  protected double evaluate(Parameters settings) throws Exception {
    String settingString = settings.toString();
    if(testedParameters.containsKey(settingString)){
      return testedParameters.get(settingString);
    }

    HashMap<String, ScoredDocument[]> resMap = new HashMap();

    // ensure the global parameters contain the current settings.
    this.retrieval.getGlobalParameters().copyFrom(settings);

    for (String number : this.queryRoots.keySet()) {
      Node root = this.queryRoots.get(number).clone();
      root = this.ensureSettings(root, settings);
      root = this.retrieval.transformQuery(root, settings);

      //  need to add queryProcessing params some extra stuff to 'settings'
      ScoredDocument[] scoredDocs = this.retrieval.runQuery(root, settings);

      if (scoredDocs != null) {
        resMap.put(number, scoredDocs);
      }
    }

    QuerySetResults results = new QuerySetResults(resMap);
    double r = evalFunction.evaluate(results, qrels);
    testedParameters.put(settingString, r);
    return r;
  }

  /**
   * normalizes parameters according to rules (sumTo x, multipyTo x, etc)
   */
  protected Parameters normalizeParameters(Parameters params) {
    // currently assuming that if there's more than one parameter - then all parameters must sum to one.
    if (this.learnableParameters.size() > 1) {
      double total = 0.0;
      for (String p : this.learnableParameters) {
        total += params.getDouble(p);
      }
      if (total != 0.0) {
        for (String p : this.learnableParameters) {
          params.set(p, params.getDouble(p) / total);
        }
      }
    }
    return params;
  }

  /**
   * This could be a traversal. This function will replace the parameters that
   * are present in the query already. Other parameters
   *
   */
  protected Node ensureSettings(Node n, Parameters settings) {
    NodeParameters np = n.getNodeParameters();
    for (String k : np.getKeySet()) {
      if (settings.containsKey(k)) {
        np.set(k, settings.getDouble(k));
      }
    }
    for (Node c : n.getInternalNodes()) {
      ensureSettings(c, settings);
    }
    return n;
  }
}

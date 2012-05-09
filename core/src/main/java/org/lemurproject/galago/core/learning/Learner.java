/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.eval.QuerySetJudgments;
import org.lemurproject.galago.core.eval.QuerySetResults;
import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluator;
import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluatorFactory;
import org.lemurproject.galago.core.retrieval.CachedRetrieval;
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
  protected LearnableQueryParameters learnableParameters;
  protected List<LearnableParameterInstance> initialSettings;

  // optimized parameters - mapping is from index of initial settings to optimal parameters
  // protected Map<Integer, Double> optimizedParameterScores;
  // protected Map<Integer, LearnableParameterInstance> optimizedParameters;
  // evaluation cache to avoid recalculating scores for known settings
  // protected Map<String, Double> testedParameters;
  public Learner(Parameters p, Retrieval r) throws Exception {
    logger = Logger.getLogger(this.getClass().getName());
    retrieval = r;
    random = (p.isLong("randomSeed")) ? new Random(p.getLong("randomSeed")) : new Random();

    initialize(p);
  }

  /**
   * learning function - returns a list of learnt parameters
   */
  public List<Parameters> learn() throws Exception {
    List<Parameters> learntParams = new ArrayList();
    for (int i = 0; i < this.initialSettings.size(); i++) {
      LearnableParameterInstance s = learn(this.initialSettings.get(i).clone());
      learntParams.add(s.toParameters());
    }
    return learntParams;
  }

  /**
   * instance learning function - should return the best parameters discovered
   * for these initial settings.
   */
  public abstract LearnableParameterInstance learn(LearnableParameterInstance initialSettings) throws Exception;

  /**
   * Getters and Setters - currently only for the initial settings
   */
  public List<LearnableParameterInstance> getInitialSettings() {
    return this.initialSettings;
  }

  public void setInitialSettings(List<LearnableParameterInstance> initialSettings) {
    this.initialSettings = initialSettings;
  }

  /**
   * UTILITY FUNCTIONS : functions that can be used inside of any implemented
   * learner
   */
  protected void initialize(Parameters p) throws IOException, Exception {

    assert (p.isString("qrels")) : this.getClass().getName() + " requires `qrels' parameter, of type String.";
    assert (p.isList("learnableParameters", Type.MAP)) : this.getClass().getName() + " requires `learnableParameters' parameter, of type List<Map>.";
    assert (!p.containsKey("normalization") || (p.isMap("normalization") || p.isList("normalization", Type.MAP))) : this.getClass().getName() + " requires `learnableParameters' parameter to be of type List<Map>.";

    this.queries = BatchSearch.collectQueries(p);
    assert !this.queries.isEmpty() : this.getClass().getName() + " requires `queries' parameter, of type List(Parameters): see Batch-Search for an example.";

    this.qrels = new QuerySetJudgments(p.getString("qrels"));
    this.evalFunction = QuerySetEvaluatorFactory.instance(p.get("metric", "map"));


    List<Parameters> params = (List<Parameters>) p.getList("learnableParameters");
    List<Parameters> normalizationRules;
    if (p.isList("normalization", Type.MAP)) {
      normalizationRules = p.getList("normalization");

      // might have forgotten to wrap rule : [{}]
    } else if (p.isMap("normalization")) {
      normalizationRules = new ArrayList();
      normalizationRules.add(p.getMap("normalization"));

    } else {
      normalizationRules = new ArrayList();
    }

    this.learnableParameters = new LearnableQueryParameters(params, normalizationRules);

    long restarts = p.get("restarts", 3);
    initialSettings = new ArrayList(3);
    if (p.isList("initialParameters", Type.MAP)) {
      for (Parameters init : (List<Parameters>) p.getList("initialParameters")) {
        LearnableParameterInstance inst = new LearnableParameterInstance(learnableParameters, init);
        initialSettings.add(inst);
      }
    }

    // now add more initial settings
    while (initialSettings.size() < restarts) {
      initialSettings.add(new LearnableParameterInstance(learnableParameters, generateRandomInitalValues()));
      logger.log(Level.INFO, "Generated initial values: {0}", initialSettings.get(initialSettings.size() - 1).toParameters().toString());
    }

    // we only want to parse queries into nodes once --> we will, however, need to transform queries repeatedly
    queryRoots = new HashMap();
    for (Parameters query : this.queries) {
      String number = query.getString("number");
      String text = query.getString("text");
      Node root = StructuredQuery.parse(text);
      queryRoots.put(number, root);
    }

    // caching system
    if (retrieval instanceof CachedRetrieval) {
      Parameters rnd1 = generateRandomInitalValues();
      Parameters rnd2 = generateRandomInitalValues();

      for (String number : this.queryRoots.keySet()) {
        Node root1 = this.queryRoots.get(number).clone();
        root1 = this.ensureSettings(root1, rnd1);
        root1 = this.retrieval.transformQuery(root1, rnd1);
        Node root2 = this.queryRoots.get(number).clone();
        root2 = this.ensureSettings(root2, rnd2);
        root2 = this.retrieval.transformQuery(root2, rnd2);
        
      }
    }
  }

  /**
   * generateRandomInitalValues
   */
  protected Parameters generateRandomInitalValues() {
    Parameters init = new Parameters();
    for (String p : this.learnableParameters.getParams()) {
      double val = random.nextDouble();
      val *= this.learnableParameters.getRange(p);
      val += this.learnableParameters.getMin(p);
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
  protected double evaluate(LearnableParameterInstance instance) throws Exception {
    // check cache for previous evaluation

    //String settingString = settings.toString();
    //if (testedParameters.containsKey(settingString)) {
    //  return testedParameters.get(settingString);
    //}

    HashMap<String, ScoredDocument[]> resMap = new HashMap();

    // ensure the global parameters contain the current settings.
    Parameters settings = instance.toParameters();
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

    // store score in cache for future reference
    // testedParameters.put(settingString, r);
    return r;
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

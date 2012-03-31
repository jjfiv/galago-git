/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.learning;

import java.util.*;
import java.util.logging.Logger;
import org.lemurproject.galago.core.eval.QuerySetJudgments;
import org.lemurproject.galago.core.eval.QuerySetResults;
import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluator;
import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluatorFactory;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;

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

  protected Logger logger;
  protected final Retrieval retrieval;
  protected final List<Parameters> queries;
  protected final Map<String,Node> queryRoots;
  protected final QuerySetJudgments qrels;
  protected final QuerySetEvaluator evalFunction;
  protected final Set<String> learnableParameters;
  protected final Map<String, Double> learnableParametersMax;
  protected final Map<String, Double> learnableParametersMin;
  protected final Map<String, Double> learnableParametersRange;
  // statistics
  protected Map<Parameters, Double> testedParameters;

  //  optional
  // protected final boolean randomizeParameterValues;
  // protected final int randomRestarts;
  // protected final List<Parameters> initialParameters;
  public Learner(Parameters p) throws Exception {
    logger = Logger.getLogger(this.getClass().getName());

    retrieval = RetrievalFactory.instance(p);
    queries = (List<Parameters>) p.getList("queries");
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

    // randomizeParameterValues = p.get("randomizeParameterValues", true);
    // randomRestarts = (int) p.get("randomRestarts", 5);
    // initialParameters = (p.containsKey("initialParameters")) ? (List<Parameters>) p.getList("initialParameters") : new ArrayList();
    

    // we only want to parse queries once.
    queryRoots = new HashMap();
    for(Parameters query : this.queries){
      String number = query.getString("number");
      String text = query.getString("text");
      Node root = StructuredQuery.parse(text);
      queryRoots.put(number, root);
    }
  }

  /**
   * main learning function - returns a local optima for the parameters
   */
  public abstract Parameters learn() throws Exception;

  /**
   * UTILITY FUNCTIONS : for any implementing learner *
   */
  /**
   * generateRandomInitalValues
   */
  public Parameters generateRandomInitalValues(Random rnd) {
    Parameters init = new Parameters();
    for (String p : this.learnableParameters) {
      double val = rnd.nextDouble();
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
  public double evaluate(Parameters settings) throws Exception {
    HashMap<String, ScoredDocument[]> resMap = new HashMap();
    for(String number : this.queryRoots.keySet()){
      // try to replace parameters here?
      Node root = this.queryRoots.get(number);
      // or try to replace parameters here?
    }
    
    QuerySetResults results = new QuerySetResults(resMap);
    evalFunction.evaluate(results, qrels);
    // for each query - run it with the parameter settings
    // collect the ScoredDocument[]s - run the set evaluator - return score.
    return 0.0;
  }
  
  public Parameters normalizeParameters(Parameters params){
    // run the normalization functions
    // e.g weight parameters might need to sum to 1.
    return params;
  }
}

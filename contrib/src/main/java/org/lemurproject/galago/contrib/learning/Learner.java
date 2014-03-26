/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import org.lemurproject.galago.core.eval.QuerySetJudgments;
import org.lemurproject.galago.core.eval.QuerySetResults;
import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluator;
import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluatorFactory;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.apps.BatchSearch;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

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
 * @author sjh
 */
public abstract class Learner {

  protected final String name;
  protected final Logger logger;
  protected final Random random;
  protected final Retrieval retrieval;
  // variable parameters
  protected QuerySet queries;
  protected QuerySetJudgments qrels;
  protected QuerySetEvaluator evalFunction;
  protected RetrievalModelParameters learnableParameters;
  protected List<RetrievalModelInstance> initialSettings;
  // evaluation cache to avoid recalculating scores for known settings
  protected Map<String, Double> testedParameters;
  // execution  
  protected int restarts;
  protected boolean threading;
  protected int threadCount;
  // output
  protected File outputFolder;
  protected final PrintStream outputPrintStream;
  protected final PrintStream outputTraceStream;

  public Learner(Parameters p, Retrieval r) throws Exception {
    logger = Logger.getLogger(this.getClass().getName());
    retrieval = r;
    random = (p.isLong("randomSeed")) ? new Random(p.getLong("randomSeed")) : new Random(System.nanoTime());
    name = p.get("name", "default");

    if (p.isString("output")) {
      outputFolder = new File(p.getString("output"));
      if (!outputFolder.isDirectory()) {
        FileUtility.makeParentDirectories(outputFolder);
        outputFolder.mkdirs();
      }
      outputPrintStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(outputFolder, name + ".out"))));
      outputTraceStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(outputFolder, name + ".trace"))));
    } else {
      outputFolder = null;
      outputPrintStream = System.out;
      outputTraceStream = System.err;
    }

    initialize(p, r);
  }

  public void close() {
    if (outputFolder != null) {
      outputPrintStream.close();
      outputTraceStream.close();
    }
  }

  /**
   * UTILITY FUNCTIONS : functions that can be used inside of any implemented
   * learner
   */
  protected void initialize(Parameters p, Retrieval r) throws Exception {

    assert (p.isString("qrels")) : this.getClass().getName() + " requires `qrels' parameter, of type String.";
    assert (p.isList("learnableParameters", Parameters.class)) : this.getClass().getName() + " requires `learnableParameters' parameter, of type List<Map>.";
    assert (!p.containsKey("normalization") || (p.isMap("normalization") || p.isList("normalization", Parameters.class))) : this.getClass().getName() + " requires `normalization' parameter to be of type List<Map>.";

    queries = new QuerySet(BatchSearch.collectQueries(p), p);
    assert !queries.isEmpty() : this.getClass().getName() + " requires `queries' parameter, of type List(Parameters): see Batch-Search for an example.";

    boolean binaryJudgments = p.get("binary", false);
    boolean positiveJudgments = p.get("postive", true);
    qrels = new QuerySetJudgments(p.getString("qrels"), binaryJudgments, positiveJudgments);
    evalFunction = QuerySetEvaluatorFactory.instance(p.get("metric", "map"), p);

    threading = p.get("threading", false);
    threadCount = (int) (p.isLong("threadCount") ? p.getLong("threadCount") : Runtime.getRuntime().availableProcessors());

    List<Parameters> params = (List<Parameters>) p.getList("learnableParameters");
    List<Parameters> normalizationRules = new ArrayList<Parameters>();
    if (p.isList("normalization", Parameters.class)
            || p.isMap("normalization")) {
      normalizationRules.addAll(p.getAsList("normalization"));
    }

    learnableParameters = new RetrievalModelParameters(params, normalizationRules);

    restarts = (int) p.get("restarts", 1);
    initialSettings = new ArrayList<RetrievalModelInstance>(restarts);
    if (p.isList("initialParameters", Parameters.class)) {
      List<Parameters> inits = (List<Parameters>) p.getAsList("initialParameters");
      for (Parameters init : inits) {
        RetrievalModelInstance inst = new RetrievalModelInstance(learnableParameters, init);
        initialSettings.add(inst);
      }
    }

    testedParameters = new HashMap<String,Double>();

    // caching system
    if (retrieval.getGlobalParameters().get("cache", false)) {
      outputTraceStream.println("Starting. Caching query nodes");
      ensureCachedQueryNodes(retrieval);
      outputTraceStream.println("Done. Caching query nodes");
    }
  }

  /**
   * learning function - returns a list of learnt parameters
   */
  public abstract RetrievalModelInstance learn() throws Exception;

  /**
   * Getters and Setters - currently only for the initial settings
   */
  public List<RetrievalModelInstance> getInitialSettings() {
    return this.initialSettings;
  }

  public void setInitialSettings(List<RetrievalModelInstance> initialSettings) {
    this.initialSettings = initialSettings;
  }

  /**
   * generateRandomInitalValues
   */
  protected RetrievalModelInstance generateRandomInitalValues() {
    Parameters init = new Parameters();
    for (String p : this.learnableParameters.getParams()) {
      double val = random.nextDouble();
      val *= this.learnableParameters.getRange(p);
      val += this.learnableParameters.getMin(p);
      init.set(p, val);
    }
    return new RetrievalModelInstance(learnableParameters, init);
  }

  /**
   * Applies new parameter settings to each query, runs the query, evaluates the
   * scored document results
   *
   * Runs all of the queries with the new parameter settings
   *
   */
  protected double evaluate(RetrievalModelInstance instance) throws Exception {
    // check cache for previous evaluation

    String settingString = instance.toString();
    if (testedParameters.containsKey(settingString)) {
      return testedParameters.get(settingString);
    }

    HashMap<String,  List<ScoredDocument>> resMap = new HashMap<String, List<ScoredDocument>>();

    // get the parameter settigns
    Parameters settings = instance.toParameters();

    long start = System.currentTimeMillis();
    double count = 0;
    for (String number : this.queries.getQueryNumbers()) {
      count += 1;
      Node root = this.queries.getNode(number).clone();

      // ensure any specifics for this query
      settings.setBackoff(this.queries.getParameters(number));

      root = this.ensureSettings(root, settings);
      root = this.retrieval.transformQuery(root, settings);

      //  need to add queryProcessing params some extra stuff to 'settings'
      List<ScoredDocument> scoredDocs = this.retrieval.executeQuery(root, settings).scoredDocuments;
      
      // now unset the backoff (next query will have different backoffs)
      settings.setBackoff(null);

      if (scoredDocs != null) {
        resMap.put(number, scoredDocs);
      }
    }
    long end = System.currentTimeMillis();

    double avgTime = (end - start) / (count);

    QuerySetResults results = new QuerySetResults(resMap);
    results.ensureQuerySet(queries.getQueryParameters());
    double r = evalFunction.evaluate(results, qrels);

    outputTraceStream.println(String.format("QuerySet run time: %d (%.3f per query), settings : %s, score : %.5f", (end - start), avgTime, settings.toString(), r));


    // store score in cache for future reference
    testedParameters.put(settingString, r);
    return r;
  }

  /**
   * This could be a traversal. This function will replace the parameters that
   * are present in the query already. Other parameters
   *
   */
  protected Node ensureSettings(Node n, Parameters settings) {
    NodeParameters np = n.getNodeParameters();
    for (String param : np.getKeySet()) {
      if (settings.containsKey(param)) {
        np.set(param, settings.getDouble(param));
      }
    }
    for (Node c : n.getInternalNodes()) {
      ensureSettings(c, settings);
    }
    return n;
  }

  private void ensureCachedQueryNodes(Retrieval cache) throws Exception {
    // generate some new random parameters

    RetrievalModelInstance rnd1 = generateRandomInitalValues();
    RetrievalModelInstance rnd2 = generateRandomInitalValues();
    Parameters settings1 = rnd1.toParameters();
    Parameters settings2 = rnd2.toParameters();


    for (String number : this.queries.getQueryNumbers()) {
      Node root1 = this.queries.getNode(number).clone();

      // ensure any query specific parameters are set
      settings1.setBackoff(this.queries.getParameters(number));

      root1 = this.ensureSettings(root1, settings1);
      root1 = this.retrieval.transformQuery(root1, settings1);
      Set<String> cachableNodes1 = new HashSet<String>();
      collectCachableNodes(root1, cachableNodes1);

      Node root2 = this.queries.getNode(number).clone();

      // ensure any query specific parameters are set
      settings2.setBackoff(this.queries.getParameters(number));

      root2 = this.ensureSettings(root2, settings2);
      root2 = this.retrieval.transformQuery(root2, settings2);
      Set<String> cachableNodes2 = new HashSet<String>();
      collectCachableNodes(root2, cachableNodes2);

      // intersect these sets
      for (String nodeString : cachableNodes1) {
        if (cachableNodes2.contains(nodeString)) {
          cache.addNodeToCache(StructuredQuery.parse(nodeString));
        }
      }
    }
  }

  private void collectCachableNodes(Node root, Set<String> nodeCache) {
    for (Node child : root.getInternalNodes()) {
      collectCachableNodes(child, nodeCache);
    }
    nodeCache.add(root.toString());
  }
}

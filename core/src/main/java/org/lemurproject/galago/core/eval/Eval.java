// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.eval;

import java.io.PrintStream;
import java.util.List;

import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluator;
import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluatorFactory;
import org.lemurproject.galago.core.eval.compare.QuerySetComparator;
import org.lemurproject.galago.core.eval.compare.QuerySetComparatorFactory;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Parameters.Type;

/**
 * Main method for retrieval evaluation.
 * Evaluates query results using a set of standard TREC metrics
 * 
 * 
 * @author trevor, sjh, jdalton
 */
public class Eval extends AppFunction {
  
  @Override
  public String getName(){
    return "eval";
  }
  
  @Override
  public String getHelpString() {
    return "galago eval <parameters>+: \n"
            + "Parameters:\n"
            + "  --judgments={filename} : [Required]  Relevance judgments for the set of queries.\n"
            + "  --baseline={filename}  : [Required]  Retrieved ranked lists from a set of queries.\n"
            + "  --treatment={filename} : [Optional]  Retrieved ranked lists from a set of queries.\n"
            + "                              If specified -> comparion evaluation - see below.\n"
            + "                              If NOT specified -> single evaluation - see below.\n"
            + "  --summary={true|false} : [Optional]  Chooses to print a summary of results - query number = \"all\"\n"
            + "                           [default=true]\n"
            + "  --details={true|false} : [Optional]  Chooses to print a detailed set of results - one set for each query.\n"
            + "                              [default=false]\n"
            + "  --metrics+<metric-name> : [Optional]  Chooses the set of metrics to evaluate over.\n"
            + "                              May be specified several times to produce a list of metrics.\n"
            + "                              Only used where the 'treatment' parameter is NOT set.\n"
            + "                              [defaults to a standard set]\n"
            + "  --comparisons+<comparison-name> : [Optional]  Chooses the set of statistical comparison methods to apply\n"
            + "                              May be specified several times to produce a list of comparisons.\n"
            + "                              Only used where the 'treatment' parameter is set.\n"
            + "                              [defaults to a standard set]\n"
            + "\n\n"
            + "Single evaluation:\n"
            + "   The first column is the query number, or 'all' for a mean of the metric over all queries.\n"
            + "   The second column is the metric, which is one of:                                        \n"
            + "       num_ret        Number of retrieved documents                                         \n"
            + "       num_rel        Number of relevant documents listed in the judgments file             \n"
            + "       num_rel_ret    Number of relevant retrieved documents                                \n"
            + "       map            Mean average precision                                                \n"
            + "       bpref          Bpref (binary preference)                                             \n"
            + "       ndcg           Normalized Discounted Cumulative Gain, computed over all documents    \n"
            + "       Pn             Precision, n document cutoff                                          \n"
            + "       ERRn           ERR, n document cutoff                                          \n"
            + "       R-prec         R-Precision                                                           \n"
            + "       recip_rank     Reciprocal Rank (precision at first relevant document)                \n"
            + "   The third column is the metric value.                                                    \n\n"
            + "Compared evaluation: \n"
            + "   The first column is the metric (e.g. averagePrecision, ndcg, etc.)\n"
            + "   The second column is the test/formula used:                                               \n"
            + "       baseline       The baseline mean (mean of the metric over all baseline queries)       \n"
            + "       treatment      The \'improved\' mean (mean of the metric over all treatment queries)  \n"
            + "       basebetter     Number of queries where the baseline outperforms the treatment.        \n"
            + "       treatbetter    Number of queries where the treatment outperforms the baseline.        \n"
            + "       equal          Number of queries where the treatment and baseline perform identically.\n"
            + "       ttest          P-value of a paired t-test.\n"
            + "       signtest       P-value of the Fisher sign test.                                       \n"
            + "       randomized      P-value of a randomized test.                                          \n"
            + "   The second column also includes difference tests.  In these tests, the null hypothesis is \n"
            + "     that the mean of the treatment is at least k times the mean of the baseline.  We run the\n"
            + "     same tests as before, but we artificially improve the baseline values by a factor of k. \n"
            + "       h-ttest-0.05    Largest value of k such that the ttest has a p-value of less than 0.5. \n"
            + "       h-signtest-0.05 Largest value of k such that the sign test has a p-value of less than 0.5. \n"
            + "       h-randomized-0.05 Largest value of k such that the randomized test has a p-value of less than 0.5. \n"
            + "       h-ttest-0.01    Largest value of k such that the ttest has a p-value of less than 0.1. \n"
            + "       h-signtest-0.01 Largest value of k such that the sign test has a p-value of less than 0.1. \n"
            + "       h-randomized-0.01 Largest value of k such that the randomized test has a p-value of less than 0.1. \n"
            + "  The third column is the value of the test.\n";
  }
  
  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    // check parameter's validity
    assert (p.isString("judgments")) : "eval requires 'judgments' parameter.";
    assert (p.isString("baseline")) : "eval requires 'baseline' parameter.";
    assert (!p.containsKey("treatment") || p.isString("treatment")) : "eval parameter 'treatment' must be a string.";
    assert (!p.containsKey("queries") || p.isList("queries", Type.MAP)) : "eval parameter 'queries' must be a list.";
    assert (!p.containsKey("summary") || p.isBoolean("summary")) : "eval parameter 'summary' must be a boolean.";
    assert (!p.containsKey("details") || p.isBoolean("details")) : "eval parameter 'details' must be a boolean.";
    assert (!p.containsKey("metrics") || p.isList("metrics", Type.STRING)) : "eval parameter 'metrics' must be a list of strings.";
    assert (p.get("summary", true) || p.get("details", false)) : "eval requires either 'summary' or 'details' to be set true.";
    assert (!p.containsKey("comparisons") || p.isList("comparisons", Type.STRING)) : "eval parameter 'comparisons' must be a list of strings.";
    
    
    QuerySetResults baseline = new QuerySetResults(p.getString("baseline"));
    QuerySetJudgments judgments = new QuerySetJudgments(p.getString("judgments"));    

    // this ensure that queries that return no documents are represented in the ranking
    if (p.isList("queries", Type.MAP)) {
      List<Parameters> queries = (List<Parameters>) p.getList("queries");
      baseline.ensureQuerySet(queries);
    }
    
    if (!p.containsKey("treatment")) {
      singleEvaluation(p, baseline, judgments, output);
    } else {
      QuerySetResults treatment = new QuerySetResults(p.getString("treatment"));

      // this ensure that queries that return no documents are represented in the ranking
      if (p.isList("queries", Type.MAP)) {
        List<Parameters> queries = (List<Parameters>) p.getList("queries");
        treatment.ensureQuerySet(queries);
      }

      comparisonEvaluation(p, baseline, treatment, judgments, output);
    }
  }

  /**
   * When run as a standalone application, this returns output 
   * very similar to that of trec_eval.  The first argument is 
   * the ranking file, and the second argument is the judgments
   * file, both in standard TREC format.
   */
  public void singleEvaluation(Parameters p, QuerySetResults results, QuerySetJudgments judgments, PrintStream output) {
    String formatString = "%2$-16s%1$3s %3$6.5f\n";
    String[] metrics = new String[]{"num_ret", "num_rel", "num_rel_ret", "map",
      "R-prec", "bpref", "recip_rank", "ndcg", "ndcg5", "ndcg10", "ndcg20", "ERR", "ERR10", "ERR20","P1",
      "P5", "P10", "P15", "P20", "P30", "P100", "P200", "P500", "P1000"};

    // override default list if specified:
    if (p.containsKey("metrics")) {
      metrics = (String[]) p.getAsList("metrics").toArray(new String[0]);
    }
    
    QuerySetEvaluator[] setEvaluators = createSetEvaluators(metrics, p);
    
    if (p.get("details", false)) {
      for (String query : results.getQueryIterator()) {
        for (QuerySetEvaluator setEvaluator : setEvaluators) {
          output.format(formatString, query, setEvaluator.getMetric(), setEvaluator.evaluate(results.get(query), judgments.get(query)));
        }
      }
    }
    
    if (p.get("summary", true)) {
      for (QuerySetEvaluator setEvaluator : setEvaluators) {
        output.format(formatString, "all", setEvaluator.getMetric(), setEvaluator.evaluate(results, judgments));
      }
    }
  }
  
  public void comparisonEvaluation(Parameters p, QuerySetResults baseline, QuerySetResults treatment, QuerySetJudgments judgments,
          PrintStream output) {
    String formatString = "%1$-20s%2$-20s%3$6.4f\n";
    
    String[] metrics = {"map", "R-prec", "bpref", "ndcg", "ndcg5", "ndcg10", "ndcg20", "P1", "P5", "P10", "P20"};
    // override default list if specified:
    if (p.containsKey("metrics")) {
      metrics = (String[]) p.getAsList("metrics").toArray(new String[0]);
    }
    
    
    String[] tests = new String[]{"baseline", "treatment", "baseBetter", "treatBetter", "equal", "ttest", "signtest", "randomized",
      "h-ttest-0.05", "h-signtest-0.05", "h-randomized-0.05", "h-ttest-0.01", "h-signtest-0.01", "h-randomized-0.01"};

    // override default list if specified:
    if (p.containsKey("comparisons")) {
      tests = (String[]) p.getAsList("comparisons").toArray(new String[0]);
    }
    
    QuerySetEvaluator[] setEvaluators = createSetEvaluators(metrics, p);
    QuerySetComparator[] setComparators = createSetComparators(tests);
    
    for (QuerySetEvaluator evaluator : setEvaluators) {
      String metricString = evaluator.getMetric();
      QuerySetEvaluation baseResults = evaluator.evaluateSet(baseline, judgments);
      QuerySetEvaluation treatResults = evaluator.evaluateSet(treatment, judgments);
      
      for (QuerySetComparator comparator : setComparators) {
        output.format(formatString, metricString, comparator.getTestName(), comparator.evaluate(baseResults, treatResults));
      }
    }
  }
  
  private QuerySetEvaluator[] createSetEvaluators(String[] metrics, Parameters p) {
    QuerySetEvaluator[] setEvaluators = new QuerySetEvaluator[metrics.length];
    for (int i = 0; i < metrics.length; i++) {
      setEvaluators[i] = QuerySetEvaluatorFactory.instance(metrics[i], p);
    }
    return setEvaluators;
  }
  
  private QuerySetComparator[] createSetComparators(String[] comparasionMetrics) {
    QuerySetComparator[] setComparators = new QuerySetComparator[comparasionMetrics.length];
    for (int i = 0; i < comparasionMetrics.length; i++) {
      setComparators[i] = QuerySetComparatorFactory.instance(comparasionMetrics[i]);
    }
    return setComparators;
  }
}

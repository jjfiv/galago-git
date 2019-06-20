// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.eval;

import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluator;
import org.lemurproject.galago.core.eval.aggregate.QuerySetEvaluatorFactory;
import org.lemurproject.galago.core.eval.compare.QuerySetComparator;
import org.lemurproject.galago.core.eval.compare.QuerySetComparatorFactory;
import org.lemurproject.galago.utility.queries.JSONQueryFormat;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Main method for retrieval evaluation. Evaluates query results using a set of
 * standard TREC metrics
 *
 * @author trevor, sjh, jdalton
 */
public class Eval extends AppFunction {

    private List<String> limitQueryIdentifiers;

    @Override
    public String getName() {
        return "eval";
    }

    @Override
    public String getHelpString() {
        return "galago eval <parameters>+: \n"
                + "Parameters:\n"
                + "  --judgments={filename}  : [Required]  Relevance judgments for the set of queries.\n"
                + "  --baseline={filename}   : [Optional]  Retrieved ranked lists from a set of queries.\n"
                + "                               If specified -> single or comparion evaluation - see below.\n"
                + "  --treatment={filename}  : [Optional]  Retrieved ranked lists from a set of queries.\n"
                + "                               If specified -> comparion evaluation - see below.\n"
                + "                               If NOT specified -> single evaluation - see below.\n"
                + "  --limit+{qid}         : [Optional]  A list of query identifiers. Runs will be subset to\n"
                + "                                        these queries before comparisons or calculations.\n"
                + "  --runs+{filename}       : [Optional]  A list of retrieved ranked lists from a particular set of queries.\n"
                + "                               If specified -> set evaluation - see below.\n"
                + "  --precision={number}    : [Optional]  If using 'runs', this is how many decimal places will be used.\n"
                + "  --summary={true|false}  : [Optional]  Chooses to print a summary of results - query number = \"all\"\n"
                + "                               [default=true]\n"
                + "  --details={true|false}  : [Optional]  Chooses to print a detailed set of results - one set for each query.\n"
                + "                               [default=false]\n"
                + "  --boosting={true|false} : [Optional]  Controls inclusion the boosted hypothesis tests.\n"
                + "                               [default=false]\n"
                + "  --metrics+<metric-name> : [Optional]  Chooses the set of metrics to evaluate over.\n"
                + "                               May be specified several times to produce a list of metrics.\n"
                + "                               Only used where the 'treatment' parameter is NOT set.\n"
                + "                               [defaults to a standard set]\n"
                + "  --comparisons+<comparison-name> : [Optional]  Chooses the set of statistical comparison methods to apply\n"
                + "                               May be specified several times to produce a list of comparisons.\n"
                + "                               Only used where the 'treatment' parameter is set.\n"
                + "                               NOTE: comparison test entries override boosting settings.\n"
                + "                               [defaults to a standard set, which does not include boosted hypothesis tests]\n"
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
                + "       ndcgn          Normalized Discounted Cumulative Gain, n document cutoff    \n"
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
                + "   The second column also includes difference (boosting) tests.  In these tests, the \n"
                + "     null hypothesis is that the mean of the treatment is at least k times the mean of the \n"
                + "     baseline.  We run the same tests as before, but we artificially improve the baseline values \n"
                + "     by a factor of k. \n"
                + "       h-ttest-0.05    Largest value of k such that the ttest has a p-value of less than 0.5. \n"
                + "       h-signtest-0.05 Largest value of k such that the sign test has a p-value of less than 0.5. \n"
                + "       h-randomized-0.05 Largest value of k such that the randomized test has a p-value of less than 0.5. \n"
                + "       h-ttest-0.01    Largest value of k such that the ttest has a p-value of less than 0.1. \n"
                + "       h-signtest-0.01 Largest value of k such that the sign test has a p-value of less than 0.1. \n"
                + "       h-randomized-0.01 Largest value of k such that the randomized test has a p-value of less than 0.1. \n"
                + "  The third column is the value of the test.\n"
                + "Set evaluation: \n"
                + "   First section is detailed, per query evaluation\n"
                + "       Each column shows a metric computed for a specific query, for a specific run\n"
                + "       Metrics are repeated, once per run.\n"
                + "   Second section is a summary of aggregate metrics over all queries\n"
                + "       First column is the run (filename)\n"
                + "       Other columns are the aggregate metric values for the run.\n"
                + "       Significant differences are computed relative to the first run.\n"
                + "       '*' indicates runs that are significantly different.\n"
                + "       Only the first comparison method is shown in the table.\n"
                + "\n";
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
        // check parameter's validity
        assert (p.isString("judgments")) : "eval requires 'judgments' parameter.";
        assert (p.isString("baseline") || p.isList("runs")) : "eval requires 'baseline' or 'runs' parameter.";
        assert (!p.containsKey("treatment") || p.isString("treatment")) : "eval parameter 'treatment' must be a string.";
        assert (!p.containsKey("queries") || p.isList("queries", Parameters.class)) : "eval parameter 'queries' must be a list.";
        assert (!p.containsKey("summary") || p.isBoolean("summary")) : "eval parameter 'summary' must be a boolean.";
        assert (!p.containsKey("details") || p.isBoolean("details")) : "eval parameter 'details' must be a boolean.";
        assert (!p.containsKey("boosting") || p.isBoolean("boosting")) : "eval parameter 'boosting' must be a boolean.";
        assert (!p.containsKey("metrics") || p.isList("metrics", String.class)) : "eval parameter 'metrics' must be a list of strings.";
        assert (p.get("summary", true) || p.get("details", false)) : "eval requires either 'summary' or 'details' to be set true.";
        assert (!p.containsKey("comparisons") || p.isList("comparisons", String.class) || p.isBoolean("comparisons")) : "eval parameter 'comparisons' must be a list of strings, or a boolean to turn it off (set only)";

        boolean binaryJudgments = p.get("binary", false);
        boolean positiveJudgments = p.get("postive", true);

        QuerySetJudgments judgments = new QuerySetJudgments(p.getString("judgments"), binaryJudgments, positiveJudgments);

        this.limitQueryIdentifiers = Collections.emptyList();
        if (p.containsKey("limit")) {
            this.limitQueryIdentifiers = new ArrayList<>();
            for (Object limit : p.getAsList("limit", Object.class)) {
                limitQueryIdentifiers.add(limit.toString());
            }
            output.println("Limit to Queries: " + limitQueryIdentifiers);
        }

        judgments.restrict(limitQueryIdentifiers);

        if (p.containsKey("runs")) {
            setEvaluation(p, judgments, output);
        } else if (!p.containsKey("treatment")) {
            singleEvaluation(p, judgments, output);
        } else {
            comparisonEvaluation(p, judgments, output);
        }
    }

    /**
     * When run as a standalone application, this returns output very similar to
     * that of trec_eval. The first argument is the ranking file, and the second
     * argument is the judgments file, both in standard TREC format.
     */
    public void singleEvaluation(Parameters p, QuerySetJudgments judgments, PrintStream output) throws IOException {

        Parameters eval = singleEvaluation(p, judgments, limitQueryIdentifiers);

        String formatString = "%2$-32s%1$3s %3$10.5f\n";

        List<String> metrics = eval.getList("_metrics", String.class);

        if (p.get("details", false)) {
            List<String> qids = new ArrayList<>(eval.getList("_qids", String.class));
            for (String qid : qids) {
                for (String metric : metrics) {
                    output.format(formatString, qid, metric, eval.getMap(qid).getDouble(metric));
                }
            }
        }

        if (p.get("summary", true)) {
            for (String metric : metrics) {
                output.format(formatString, "all", metric, eval.getMap("all").getDouble(metric));
            }
        }
    }

    /**
     * When run as a standalone application, this returns comparison output
     */
    public void comparisonEvaluation(Parameters p, QuerySetJudgments judgments, PrintStream output) throws IOException {

        Parameters eval = comparisonEvaluation(p, judgments, limitQueryIdentifiers);

        String formatString = "%1$-32s%2$-20s%3$10.4f\n";

        List<String> metrics = eval.getList("_metrics", String.class);
        List<String> comparisons = eval.getList("_comparisons", String.class);

        for (String metric : metrics) {
            for (String comparison : comparisons) {
                output.format(formatString, metric, comparison, eval.getMap(metric).getDouble(comparison));
            }
        }
    }

    /**
     * When run as a standalone application, this returns comparison output
     */
    public void setEvaluation(Parameters p, QuerySetJudgments judgments, PrintStream output) throws IOException {

        Parameters eval = setEvaluation(p, judgments, limitQueryIdentifiers);

        List<String> runs = eval.getList("_runs", String.class);
        List<String> metrics = eval.getList("_metrics", String.class);
        List<String> comparisons = eval.getList("_comparisons", String.class);
        List<String> qids = eval.getList("_qid", String.class);

        String sep = p.get("sep", " ");   // latex : " & "
        String ln = p.get("ln", " ");     // latex : " \\\\ \\hline"
        String sig = p.get("sig", "*");   // latex : " ^{+} "
        String neg = p.get("negsig", ""); // latex : " ^{-} "
        Integer iprec = p.get("precision", 3);
        String prec = iprec.toString();

        double thresh = p.get("thresh", 0.05);

        if (p.get("details", false)) {
            // preamble1:    | run-1 | run 2 | ...
//      output.format("%1$-10s", "");
//      for (String runId : runs) {
//        // w is the width of the set of metrics used for a given run.
//        int w = ((10 + sep.length()) * metrics.size()) - sep.length();
//        output.format("%s%2$-" + w + "s", sep, runId);
//      }
//      output.format("%s\n", ln);

            // preamble2: qids | run-1-metric 1 | run-1-metric 2 | ...
            output.format("%1$-10s", "qids");
            for (String runId : runs) {
                for (String metric : metrics) {
                    output.format("%s%s-%s", sep, runId, metric);
                }
            }
            output.format("%s\n", ln);

            String detailedFormatString = "%s%2$10." + prec + "f";

            for (String qid : qids) {
                output.format("%1$-10s", qid, sep);
                for (String runId : runs) {
                    for (String metric : metrics) {
                        output.format(detailedFormatString, sep, eval.getMap(qid).getMap(runId).getDouble(metric));
                    }
                }
                output.format("%s\n", ln);
            }
            output.format("\n\n\n");
        }

        if (p.get("summary", true)) {
            output.format("%1$-10s", "run-id");
            for (String metric : metrics) {
                output.format("%s%2$10s ", sep, metric);
            }
            output.format("%s\n", ln);

            String summaryFormatString = "%1s%2$10." + prec + "f%3$3s";

            for (String runId : runs) {
                output.format("%1$-30s", runId);
                Parameters r = eval.getMap("all").getMap(runId);
                for (String metric : metrics) {
                    if (comparisons.size() > 0
                            && r.getDouble(metric + "-" + comparisons.get(0)) < thresh) {
                        output.format(summaryFormatString, sep, r.getDouble(metric), sig);
                    } else if (neg.length() > 0
                            && comparisons.size() > 0
                            && r.getDouble(metric + "-" + comparisons.get(0) + "-neg") < thresh) {
                        output.format(summaryFormatString, sep, r.getDouble(metric), neg);

                    } else {
                        output.format(summaryFormatString, sep, r.getDouble(metric), "");
                    }
                }
                output.format("%s\n", ln);
            }
            if (comparisons.size() > 0) {
                output.format("\nSig-Test: %s, threshold set to %f\n", comparisons.get(0), thresh);
                if (comparisons.size() > 1) {
                    output.format("Other tests, computed, but not reported here: ");
                    for (int testId = 1; testId < comparisons.size(); testId++) {
                        if (testId > 1) {
                            output.format(", %s", comparisons.get(testId));
                        } else {
                            output.format("%s", comparisons.get(testId));
                        }
                    }
                    output.format("\n");
                }
            }
        }
    }


    public static String[] DefaultMetrics = new String[]{
            "num_ret", "num_rel", "num_rel_ret", "num_unjug_ret@20",
            "map", "R-prec", "bpref", "recip_rank",
            "ndcg", "ndcg5", "ndcg10", "ndcg20",
            "ERR", "ERR10", "ERR20",
            "P5", "P10", "P15", "P20", "P30", "P100", "P200", "P500", "P1000"
    };

    /**
     * These two methods allow for programmatic ways to receive test results without having to read it off
     * a print stream.
     */
    public static Parameters singleEvaluation(Parameters p, QuerySetJudgments judgments, List<String> limitQueryIdentifiers) throws IOException {

        QuerySetResults results = new QuerySetResults(p.getString("baseline"));
        results.restrictQueries(limitQueryIdentifiers);

        // this ensure that queries that return no documents are represented in the ranking
        List<Parameters> queries = JSONQueryFormat.collectQueries(p);
        if (!queries.isEmpty()) {
            results.ensureQuerySet(queries);
        }

        String[] metrics = DefaultMetrics;

        // override default list if specified:
        if (p.containsKey("metrics")) {
            metrics = (String[]) p.getAsList("metrics").toArray(new String[0]);
        }

        QuerySetEvaluator[] setEvaluators = createSetEvaluators(metrics, p);

        Parameters recorded = Parameters.create();
        recorded.set("_metrics", new ArrayList<>(Arrays.asList(metrics)));

        Parameters qRecord;
        if (p.get("details", false)) {
            List<String> qids = new ArrayList<>();
            for (String query : results.getQueryIterator()) {
                qRecord = Parameters.create();
                for (QuerySetEvaluator setEvaluator : setEvaluators) {
                    qRecord.set(setEvaluator.getMetric(), setEvaluator.evaluate(results.get(query), judgments.get(query)));
                }
                recorded.set(query, qRecord);
                qids.add(query);
            }
            recorded.set("_qids", qids);
        }

        if (p.get("summary", true)) {
            qRecord = Parameters.create();
            for (QuerySetEvaluator setEvaluator : setEvaluators) {
                qRecord.set(setEvaluator.getMetric(), setEvaluator.evaluate(results, judgments));
            }
            recorded.set("all", qRecord);
        }

        return recorded;
    }


    public static String[] DefaultComparisonMetrics = new String[]{
            "map", "R-prec", "bpref",
            "ndcg", "ndcg5", "ndcg10", "ndcg20",
            "P5", "P10", "P20"
    };

    public static Parameters comparisonEvaluation(Parameters p, QuerySetJudgments judgments, List<String> limitQueryIdentifiers) throws IOException {
        QuerySetResults baseline = new QuerySetResults(p.getString("baseline"));
        baseline.restrictQueries(limitQueryIdentifiers);
        QuerySetResults treatment = new QuerySetResults(p.getString("treatment"));
        treatment.restrictQueries(limitQueryIdentifiers);

        // this ensure that queries that return no documents are represented in the ranking
        List<Parameters> queries = JSONQueryFormat.collectQueries(p);
        if (!queries.isEmpty()) {
            baseline.ensureQuerySet(queries);
            treatment.ensureQuerySet(queries);
        }


        String[] metrics = DefaultComparisonMetrics;
        // override default list if specified:
        if (p.containsKey("metrics")) {
            metrics = (String[]) p.getAsList("metrics").toArray(new String[0]);
        }

        String[] defaultTests = new String[]{"baseline", "treatment", "baseBetter", "treatBetter", "equal", "ttest", "signtest", "randomized"};
        String[] tests = defaultTests;

        //- Let's not bother with boosted hypothesis tests unless someone really wants them
        if (p.containsKey("boosting")) {
            if (p.get("boosting", false)) {
                String[] boostTests = new String[]{"h-ttest-0.05", "h-signtest-0.05", "h-randomized-0.05",
                        "h-ttest-0.01", "h-signtest-0.01", "h-randomized-0.01"};
                tests = new String[defaultTests.length + boostTests.length];
                System.arraycopy(defaultTests, 0, tests, 0, defaultTests.length);
                System.arraycopy(boostTests, 0, tests, defaultTests.length, boostTests.length);
            }
        }

        // override default list if specified:
        if (p.containsKey("comparisons")) {
            tests = (String[]) p.getAsList("comparisons").toArray(new String[0]);
        }

        QuerySetEvaluator[] setEvaluators = createSetEvaluators(metrics, p);
        QuerySetComparator[] setComparators = createSetComparators(tests);
        Parameters recorded = Parameters.create();

        recorded.set("_metrics", new ArrayList<>(Arrays.asList(metrics)));
        recorded.set("_comparisons", new ArrayList<>(Arrays.asList(tests)));

        for (QuerySetEvaluator evaluator : setEvaluators) {
            String metricString = evaluator.getMetric();
            QuerySetEvaluation baseResults = evaluator.evaluateSet(baseline, judgments);
            QuerySetEvaluation treatResults = evaluator.evaluateSet(treatment, judgments);
            Parameters mRecord = Parameters.create();

            for (QuerySetComparator comparator : setComparators) {
                mRecord.set(comparator.getTestName(), comparator.evaluate(baseResults, treatResults));
            }
            recorded.set(metricString, mRecord);
        }

        return recorded;
    }

    public static Parameters setEvaluation(Parameters p, QuerySetJudgments judgments, List<String> limitQueryIdentifiers) throws IOException {
        // this ensure that queries that return no documents are represented in the ranking
        List<Parameters> queries = JSONQueryFormat.collectQueries(p);

        List<String> runFiles = p.getAsList("runs", String.class);
        List<QuerySetResults> runs = new ArrayList<>();
        List<String> runIds = new ArrayList<>();
        for (String runFile : runFiles) {
            QuerySetResults res = new QuerySetResults(runFile);
            res.restrictQueries(limitQueryIdentifiers);
            if (!queries.isEmpty()) {
                res.ensureQuerySet(queries);
            }
            runs.add(res);
            runIds.add(res.getName());
        }

        String[] metrics = new String[]{"map", "ndcg20", "P20"};

        // override default list if specified:
        if (p.containsKey("metrics")) {
            metrics = p.getAsList("metrics", String.class).toArray(new String[0]);
        }

        String[] tests = new String[]{"randomized"};
        boolean negsig = !p.get("negsig", "").isEmpty();

        // override default list if specified:
        if (p.isList("comparisons", String.class)) {
            tests = (String[]) p.getAsList("comparisons").toArray(new String[0]);

        } else if (p.isBoolean("comparisons") && !p.getBoolean("comparisons")) {
            // allow the comparisons to be turned off.
            tests = new String[0];
        }
        // get the test used

        List<String> qids = new ArrayList<>();
        for (String qid : runs.get(0).getQueryIterator()) {
            qids.add(qid);
        }

        QuerySetEvaluator[] setEvaluators = createSetEvaluators(metrics, p);
        QuerySetComparator[] setComparators = createSetComparators(tests);

        Parameters recorded = Parameters.create();
        recorded.set("_runs", runIds);
        recorded.set("_metrics", new ArrayList<>(Arrays.asList(metrics)));
        recorded.set("_comparisons", Arrays.asList(tests));
        recorded.set("_qid", qids);

        Parameters qRecord, qrRecord;
        if (p.get("details", false)) {
            for (String qid : qids) {
                qRecord = Parameters.create();

                for (QuerySetResults run : runs) {
                    String runName = run.getName();
                    qrRecord = Parameters.create();

                    for (QuerySetEvaluator setEvaluator : setEvaluators) {
                        qrRecord.set(setEvaluator.getMetric(), setEvaluator.evaluate(run.get(qid), judgments.get(qid)));
                    }
                    qRecord.set(runName, qrRecord);
                }
                recorded.set(qid, qRecord);
            }
        }

        if (p.get("summary", true)) {
            Parameters all = Parameters.create();
            QuerySetResults baseline = runs.get(0);

            for (QuerySetResults run : runs) {
                qRecord = Parameters.create();
                for (QuerySetEvaluator setEvaluator : setEvaluators) {
                    qRecord.set(setEvaluator.getMetric(), setEvaluator.evaluate(run, judgments));

                    for (int testId = 0; testId < tests.length; testId++) {
                        if (baseline.getName().equals(run.getName())) {
                            qRecord.set(setEvaluator.getMetric() + "-" + tests[testId], 1.0);
                            if (negsig) {
                                qRecord.set(setEvaluator.getMetric() + "-" + tests[testId] + "-neg", 1.0);
                            }
                        } else {
                            QuerySetEvaluation baseResults = setEvaluator.evaluateSet(baseline, judgments);
                            QuerySetEvaluation treatResults = setEvaluator.evaluateSet(run, judgments);
                            qRecord.set(setEvaluator.getMetric() + "-" + tests[testId], setComparators[testId].evaluate(baseResults, treatResults));
                            if (negsig) {
                                qRecord.set(setEvaluator.getMetric() + "-" + tests[testId] + "-neg", setComparators[testId].evaluate(treatResults, baseResults));
                            }
                        }
                    }
                }
                all.set(run.getName(), qRecord);
            }
            recorded.set("all", all);
        }

        return recorded;
    }

    private static QuerySetEvaluator[] createSetEvaluators(String[] metrics, Parameters p) {
        QuerySetEvaluator[] setEvaluators = new QuerySetEvaluator[metrics.length];
        for (int i = 0; i < metrics.length; i++) {
            setEvaluators[i] = QuerySetEvaluatorFactory.create(metrics[i], p);
        }
        return setEvaluators;
    }

    private static QuerySetComparator[] createSetComparators(String[] comparasionMetrics) {
        QuerySetComparator[] setComparators = new QuerySetComparator[comparasionMetrics.length];
        for (int i = 0; i < comparasionMetrics.length; i++) {
            setComparators[i] = QuerySetComparatorFactory.create(comparasionMetrics[i]);
        }
        return setComparators;
    }
}

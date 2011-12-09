// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.eval;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import org.lemurproject.galago.core.eval.RetrievalEvaluator.Document;
import org.lemurproject.galago.core.eval.RetrievalEvaluator.Judgment;

/**
 *
 * @author trevor
 */
public class Main {
    /**
     * Loads a TREC judgments file.
     *
     * @param filename The filename of the judgments file to load.
     * @return Maps from query numbers to lists of judgments for each query.
     */
    public static TreeMap<String, ArrayList<Judgment>> loadJudgments(String filename) throws IOException, FileNotFoundException {
        // open file
        BufferedReader in = new BufferedReader(new FileReader(filename));
        String line = null;
        TreeMap<String, ArrayList<Judgment>> judgments = new TreeMap<String, ArrayList<Judgment>>();
        String recentQuery = null;
        ArrayList<Judgment> recentJudgments = null;

        while ((line = in.readLine()) != null) {
            int[] columns = splits(line, 4);

            String number = line.substring(columns[0], columns[1]);
            String unused = line.substring(columns[2], columns[3]);
            String docno = line.substring(columns[4], columns[5]);
            String judgment = line.substring(columns[6]);

            Judgment j = new Judgment(docno, Integer.valueOf(judgment));

            if (recentQuery == null || !recentQuery.equals(number)) {
                if (!judgments.containsKey(number)) {
                    judgments.put(number, new ArrayList<Judgment>());
                }

                recentJudgments = judgments.get(number);
                recentQuery = number;
            }

            recentJudgments.add(j);
        }

        in.close();
        return judgments;
    }

    private static int[] splits(String s, int columns) {
        int[] result = new int[2 * columns];
        boolean lastWs = true;
        int column = 0;
        result[0] = 0;

        for (int i = 0; i < s.length() && column < columns; i++) {
            char c = s.charAt(i);
            boolean isWs = (c == ' ') || (c == '\t');

            if (!isWs && lastWs) {
                result[2 * column] = i;
            } else if (isWs && !lastWs) {
                result[2 * column + 1] = i;
                column++;
            }

            lastWs = isWs;
        }

        return result;
    }

    /**
     * Reads in a TREC ranking file.
     *
     * @param filename The filename of the ranking file.
     * @return A map from query numbers to document ranking lists.
     */
    public static TreeMap<String, ArrayList<Document>> loadRanking(String filename) throws IOException, FileNotFoundException {
        // open file
        BufferedReader in = new BufferedReader(new FileReader(filename), 256 * 1024);
        String line = null;
        TreeMap<String, ArrayList<Document>> ranking = new TreeMap<String, ArrayList<Document>>();
        ArrayList<Document> recentRanking = null;
        String recentQuery = null;

        while ((line = in.readLine()) != null) {
            int[] splits = splits(line, 6);

            // 1 Q0 WSJ880711-0086 39 -3.05948 Exp

            String number = line.substring(splits[0], splits[1]);
            String unused = line.substring(splits[2], splits[3]);
            String docno = line.substring(splits[4], splits[5]);
            String rank = line.substring(splits[6], splits[7]);
            String score = line.substring(splits[8], splits[9]);
            String runtag = line.substring(splits[10]);

            Document document = new Document(docno, Integer.valueOf(rank), Double.valueOf(score));

            if (recentQuery == null || !recentQuery.equals(number)) {
                if (!ranking.containsKey(number)) {
                    ranking.put(number, new ArrayList<Document>());
                }

                recentQuery = number;
                recentRanking = ranking.get(number);
            }

            recentRanking.add(document);
        }

        in.close();
        return ranking;
    }

    /**
     * Creates a SetRetrievalEvaluator from data from loadRanking and loadJudgments.
     */
    public static SetRetrievalEvaluator create(TreeMap<String, ArrayList<Document>> allRankings, TreeMap<String, ArrayList<Judgment>> allJudgments) {
        TreeMap<String, RetrievalEvaluator> evaluators = new TreeMap<String, RetrievalEvaluator>();

        for (String query : allRankings.keySet()) {
            ArrayList<Judgment> judgments = allJudgments.get(query);
            ArrayList<Document> ranking = allRankings.get(query);

            if (judgments == null || ranking == null) {
                continue;
            }

            RetrievalEvaluator evaluator = new RetrievalEvaluator(query, ranking, judgments);
            evaluators.put(query, evaluator);
        }

        return new SetRetrievalEvaluator(evaluators.values());
    }

    /**
     * When run as a standalone application, this returns output 
     * very similar to that of trec_eval.  The first argument is 
     * the ranking file, and the second argument is the judgments
     * file, both in standard TREC format.
     */
    public static void singleEvaluation(SetRetrievalEvaluator setEvaluator, PrintStream output) {
        String formatString = "%2$-16s%1$3s ";

        // print trec_eval relational-style output
        for (RetrievalEvaluator evaluator : setEvaluator.getEvaluators()) {
            String query = evaluator.queryName();

            // counts
            output.format(formatString + "%3$d\n", query, "num_ret", evaluator.
                              retrievedDocuments().size());
            output.format(formatString + "%3$d\n", query, "num_rel", evaluator.relevantDocuments().
                              size());
            output.format(formatString + "%3$d\n", query, "num_rel_ret", evaluator.
                              relevantRetrievedDocuments().size());

            // aggregate measures
            output.format(formatString + "%3$6.4f\n", query, "map", evaluator.averagePrecision());
            output.format(formatString + "%3$6.4f\n", query, "ndcg", evaluator.
                              normalizedDiscountedCumulativeGain());
            output.format(formatString + "%3$6.4f\n", query, "ndcg15", evaluator.
                              normalizedDiscountedCumulativeGain(15));
            output.format(formatString + "%3$6.4f\n", query, "R-prec", evaluator.rPrecision());
            output.format(formatString + "%3$6.4f\n", query, "bpref",
                              evaluator.binaryPreference());
            output.format(formatString + "%3$6.4f\n", query, "recip_rank", evaluator.
                              reciprocalRank());

            // precision at fixed points
            int[] fixedPoints = {5, 10, 15, 20, 30, 100, 200, 500, 1000};

            for (int i = 0; i < fixedPoints.length; i++) {
                int point = fixedPoints[i];
                output.format(formatString + "%3$6.4f\n", query, "P" + point, evaluator.
                                  precision(fixedPoints[i]));
            }
        }

        // print summary data
        output.format(formatString + "%3$d\n", "all", "num_ret", setEvaluator.numberRetrieved());
        output.format(formatString + "%3$d\n", "all", "num_rel", setEvaluator.numberRelevant());
        output.format(formatString + "%3$d\n", "all", "num_rel_ret", setEvaluator.
                          numberRelevantRetrieved());

        output.format(formatString + "%3$6.4f\n", "all", "map", setEvaluator.
                          meanAveragePrecision());
        output.format(formatString + "%3$6.4f\n", "all", "ndcg", setEvaluator.
                          meanNormalizedDiscountedCumulativeGain());
        output.format(formatString + "%3$6.4f\n", "all", "ndcg15", setEvaluator.
                          meanNormalizedDiscountedCumulativeGain(15));
        output.format(formatString + "%3$6.4f\n", "all", "R-prec", setEvaluator.meanRPrecision());
        output.format(formatString + "%3$6.4f\n", "all", "bpref", setEvaluator.
                          meanBinaryPreference());
        output.format(formatString + "%3$6.4f\n", "all", "recip_rank", setEvaluator.
                          meanReciprocalRank());

        // precision at fixed points
        int[] fixedPoints = {5, 10, 15, 20, 30, 100, 200, 500, 1000};

        for (int i = 0; i < fixedPoints.length; i++) {
            int point = fixedPoints[i];
            output.format(formatString + "%3$6.4f\n", "all", "P" + point, setEvaluator.
                              meanPrecision(fixedPoints[i]));
        }
    }

    /**
     * Compares two ranked lists with statistical tests on most major metrics.
     */
    public static void comparisonEvaluation(SetRetrievalEvaluator baseline,
                                            SetRetrievalEvaluator treatment,
                                            boolean useRandomized,
                                            PrintStream output) {
        String[] metrics = {"averagePrecision", "ndcg", "ndcg15", "bpref", "P10", "P20"};
        String formatString = "%1$-20s%2$-20s%3$6.4f\n";
        String integerFormatString = "%1$-20s%2$-20s%3$d\n";

        for (String metric : metrics) {
            Map<String, Double> baselineMetric = baseline.evaluateAll(metric);
            Map<String, Double> treatmentMetric = treatment.evaluateAll(metric);

            SetRetrievalComparator comparator = new SetRetrievalComparator(baselineMetric,
                                                                           treatmentMetric);

            output.format(formatString, metric, "baseline", comparator.meanBaselineMetric());
            output.format(formatString, metric, "treatment", comparator.meanTreatmentMetric());

            output.format(integerFormatString, metric, "basebetter", comparator.
                              countBaselineBetter());
            output.format(integerFormatString, metric, "treatbetter", comparator.
                              countTreatmentBetter());
            output.format(integerFormatString, metric, "equal", comparator.countEqual());

            output.format(formatString, metric, "ttest", comparator.pairedTTest());
            output.format(formatString, metric, "signtest", comparator.signTest());
            if (useRandomized) {
                output.format(formatString, metric, "randomized", comparator.
                                                 randomizedTest());
            }
            output.format(formatString, metric, "h-ttest-0.05", comparator.supportedHypothesis(
                              "ttest", 0.05));
            output.format(formatString, metric, "h-signtest-0.05", comparator.
                              supportedHypothesis("sign", 0.05));
            if (useRandomized) {
                output.format(formatString, metric, "h-randomized-0.05",
                                                 comparator.supportedHypothesis("randomized", 0.05));
            }
            output.format(formatString, metric, "h-ttest-0.01", comparator.supportedHypothesis(
                              "ttest", 0.01));
            output.format(formatString, metric, "h-signtest-0.01", comparator.
                              supportedHypothesis("sign", 0.01));
            if (useRandomized) {
                output.format(formatString, metric, "h-randomized-0.01",
                                                 comparator.supportedHypothesis("randomized", 0.01));
            }
        }
    }

    public static void usage(PrintStream output) {
        output.println("galago eval <args>: ");
        output.println(
                "   There are two ways to use this program.  First, you can evaluate a single ranking: ");
        output.println("      galago eval TREC-Ranking-File TREC-Judgments-File");
        output.println("   or, you can use it to compare two rankings with statistical tests: ");
        output.println(
                "      galago eval TREC-Baseline-Ranking-File TREC-Improved-Ranking-File TREC-Judgments-File");
        output.println("   you can also include randomized tests (these take a bit longer): ");
        output.println(
                "      galago eval TREC-Baseline-Ranking-File TREC-Treatment-Ranking-File TREC-Judgments-File randomized");
        output.println();
        output.println("Single evaluation:");
        output.println(
                "   The first column is the query number, or 'all' for a mean of the metric over all queries.");
        output.println(
                "   The second column is the metric, which is one of:                                        ");
        output.println(
                "       num_ret        Number of retrieved documents                                         ");
        output.println(
                "       num_rel        Number of relevant documents listed in the judgments file             ");
        output.println(
                "       num_rel_ret    Number of relevant retrieved documents                                ");
        output.println(
                "       map            Mean average precision                                                ");
        output.println(
                "       bpref          Bpref (binary preference)                                             ");
        output.println(
                "       ndcg           Normalized Discounted Cumulative Gain, computed over all documents    ");
        output.println(
                "       ndcg15         Normalized Discounted Cumulative Gain, 15 document cutoff             ");
        output.println(
                "       Pn             Precision, n document cutoff                                          ");
        output.println(
                "       R-prec         R-Precision                                                           ");
        output.println(
                "       recip_rank     Reciprocal Rank (precision at first relevant document)                ");
        output.println(
                "   The third column is the metric value.                                                    ");
        output.println();
        output.println("Compared evaluation: ");
        output.println("   The first column is the metric (e.g. averagePrecision, ndcg, etc.)");
        output.println(
                "   The second column is the test/formula used:                                               ");
        output.println(
                "       baseline       The baseline mean (mean of the metric over all baseline queries)       ");
        output.println(
                "       treatment      The \'improved\' mean (mean of the metric over all treatment queries)  ");
        output.println(
                "       basebetter     Number of queries where the baseline outperforms the treatment.        ");
        output.println(
                "       treatbetter    Number of queries where the treatment outperforms the baseline.        ");
        output.println(
                "       equal          Number of queries where the treatment and baseline perform identically.");
        output.println(
                "       ttest          P-value of a paired t-test.");
        output.println(
                "       signtest       P-value of the Fisher sign test.                                       ");
        output.println(
                "       randomized      P-value of a randomized test.                                          ");

        output.println(
                "   The second column also includes difference tests.  In these tests, the null hypothesis is ");
        output.println(
                "     that the mean of the treatment is at least k times the mean of the baseline.  We run the");
        output.println(
                "     same tests as before, but we artificially improve the baseline values by a factor of k. ");

        output.println(
                "       h-ttest-0.05    Largest value of k such that the ttest has a p-value of less than 0.5. ");
        output.println(
                "       h-signtest-0.05 Largest value of k such that the sign test has a p-value of less than 0.5. ");
        output.println(
                "       h-randomized-0.05 Largest value of k such that the randomized test has a p-value of less than 0.5. ");

        output.println(
                "       h-ttest-0.01    Largest value of k such that the ttest has a p-value of less than 0.1. ");
        output.println(
                "       h-signtest-0.01 Largest value of k such that the sign test has a p-value of less than 0.1. ");
        output.println(
                "       h-randomized-0.01 Largest value of k such that the randomized test has a p-value of less than 0.1. ");
        output.println();

        output.println(
                "  The third column is the value of the test.");
    }

    public static void internalMain(String[] args, PrintStream output) throws IOException {
        if (args.length >= 3) {
            TreeMap<String, ArrayList<Document>> baselineRanking = loadRanking(args[0]);
            TreeMap<String, ArrayList<Document>> treatmentRanking = loadRanking(args[1]);
            TreeMap<String, ArrayList<Judgment>> judgments = loadJudgments(args[2]);

            SetRetrievalEvaluator baseline = create(baselineRanking, judgments);
            SetRetrievalEvaluator treatment = create(treatmentRanking, judgments);
            boolean useRandomized = args.length >= 4;

            comparisonEvaluation(baseline, treatment, useRandomized, output);
        } else if (args.length == 2) {
            TreeMap<String, ArrayList<Document>> ranking = loadRanking(args[0]);
            TreeMap<String, ArrayList<Judgment>> judgments = loadJudgments(args[1]);

            SetRetrievalEvaluator setEvaluator = create(ranking, judgments);
            singleEvaluation(setEvaluator, output);
        } else {
            usage(output);
        }
    }

    public static void main(String[] args) throws IOException {
        try {
            internalMain(args, System.out);
        } catch (Exception e) {
            e.printStackTrace();
            usage(System.out);
        }
    }
}

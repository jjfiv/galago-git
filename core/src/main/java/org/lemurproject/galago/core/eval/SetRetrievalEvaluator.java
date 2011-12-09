// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.eval;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * Computes summary statistics over a set of queries.
 *
 * @author Trevor Strohman
 */
public class SetRetrievalEvaluator {
    Collection<RetrievalEvaluator> _evaluators;

    /** Creates a new instance of SetRetrievalEvaluator */
    public SetRetrievalEvaluator(Collection<RetrievalEvaluator> evaluators) {
        _evaluators = evaluators;
    }

    /**
     * Returns a collection of evaluators.
     */
    public Collection<RetrievalEvaluator> getEvaluators() {
        return _evaluators;
    }

    /**
     * Returns the mean average precision; the mean of the average
     * precision values for all queries.
     */
    public double meanAveragePrecision() {
        double sumAveragePrecision = 0;

        for (RetrievalEvaluator evaluator : _evaluators) {
            sumAveragePrecision += evaluator.averagePrecision();
        }

        return sumAveragePrecision / (double) _evaluators.size();
    }

    /**
     * Returns the mean of the binary preference values
     * for all queries.
     */
    public double meanBinaryPreference() {
        double sumBinaryPreference = 0;

        for (RetrievalEvaluator evaluator : _evaluators) {
            sumBinaryPreference += evaluator.binaryPreference();
        }

        return sumBinaryPreference / (double) _evaluators.size();
    }

    /**
     * Returns the geometric mean of average precision values
     * for all queries.
     */
    public double geometricMeanAveragePrecision() {
        double productAveragePrecision = 0;

        for (RetrievalEvaluator evaluator : _evaluators) {
            productAveragePrecision *= evaluator.averagePrecision();
        }

        return Math.pow(productAveragePrecision, 1.0 / _evaluators.size());
    }

    /**
     * Returns the mean of the precision values
     * for all queries.
     */
    public double meanPrecision(int documentsRetrieved) {
        double sumPrecision = 0;

        for (RetrievalEvaluator evaluator : _evaluators) {
            sumPrecision += evaluator.precision(documentsRetrieved);
        }

        return sumPrecision / _evaluators.size();
    }

    /** 
     * Returns the mean of the reciprocal rank values for all queries.
     */
    public double meanReciprocalRank() {
        double sumRR = 0;

        for (RetrievalEvaluator evaluator : _evaluators) {
            sumRR += evaluator.reciprocalRank();
        }

        return sumRR / _evaluators.size();
    }

    /** 
     * Returns the mean of the R-precision values for all queries.
     */
    public double meanRPrecision() {
        double sumRPrecision = 0;

        for (RetrievalEvaluator evaluator : _evaluators) {
            sumRPrecision += evaluator.rPrecision();
        }

        return sumRPrecision / _evaluators.size();
    }

    /**
     * Returns the mean of the NDCG values for all queries.
     */
    public double meanNormalizedDiscountedCumulativeGain() {
        double sumNDCG = 0;

        for (RetrievalEvaluator evaluator : _evaluators) {
            sumNDCG += evaluator.normalizedDiscountedCumulativeGain();
        }

        return sumNDCG / _evaluators.size();
    }

    /**
     * Returns the mean of the NDCG values for all queries.
     */
    public double meanNormalizedDiscountedCumulativeGain(int documentsRetrieved) {
        double sumNDCG = 0;

        for (RetrievalEvaluator evaluator : _evaluators) {
            sumNDCG += evaluator.normalizedDiscountedCumulativeGain(documentsRetrieved);
        }

        return sumNDCG / _evaluators.size();
    }

    /**
     * Returns a Map containing a particular metric value for each query.
     * For instance, if metric == "averagePrecision", this returns 
     * a map where the keys are query identifiers and the values are the
     * average precision metric evaluated for each query.
     */
    public Map<String, Double> evaluateAll(String metric) throws IllegalArgumentException {
        TreeMap<String, Double> result = new TreeMap<String, Double>();

        for (RetrievalEvaluator evaluator : _evaluators) {
            double value = 0;

            if (metric.equals("averagePrecision") || metric.equals("ap") || metric.equals("map")) {
                value = evaluator.averagePrecision();
            } else if (metric.equals("ndcg")) {
                value = evaluator.normalizedDiscountedCumulativeGain();
            } else if (metric.startsWith("ndcg")) {
                value = evaluator.normalizedDiscountedCumulativeGain(Integer.parseInt(metric.
                                                                                      substring(4)));
            } else if (metric.equals("reciprocalRank") || metric.equals("mrr")) {
                value = evaluator.reciprocalRank();
            } else if (metric.equals("rPrecision")) {
                value = evaluator.rPrecision();
            } else if (metric.equals("bpref")) {
                value = evaluator.binaryPreference();
            } else if (metric.startsWith("P")) {
                value = evaluator.precision(Integer.parseInt(metric.substring(1)));
            } else if (metric.startsWith("R")) {
                value = evaluator.recall(Integer.parseInt(metric.substring(1)));
            } else {
                throw new IllegalArgumentException("Unknown metric: " + metric);
            }

            result.put(evaluator.queryName(), value);
        }

        return result;
    }

    /**
     * The number of documents retrieved for all queries.
     */
    public int numberRetrieved() {
        int sumRetrieved = 0;

        for (RetrievalEvaluator evaluator : _evaluators) {
            sumRetrieved += evaluator.retrievedDocuments().size();
        }

        return sumRetrieved;
    }

    /**
     * The total number of relevant documents to any of the queries.
     */
    public int numberRelevant() {
        int sumRelevant = 0;

        for (RetrievalEvaluator evaluator : _evaluators) {
            sumRelevant += evaluator.relevantDocuments().size();
        }

        return sumRelevant;
    }

    /**
     * The total number of relevant documents retrieved for any of the queries.
     */
    public int numberRelevantRetrieved() {
        int sumRelevantRetrieved = 0;

        for (RetrievalEvaluator evaluator : _evaluators) {
            sumRelevantRetrieved += evaluator.relevantRetrievedDocuments().size();
        }

        return sumRelevantRetrieved;
    }
}

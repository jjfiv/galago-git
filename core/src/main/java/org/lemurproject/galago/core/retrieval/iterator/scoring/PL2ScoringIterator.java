/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.TransformIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements PL2 retrieval model from the DFR framework.
 *
 * @author sjh
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount", "nodeFrequency", "maximumCount"})
@RequiredParameters(parameters = {"c"})
public class PL2ScoringIterator extends TransformIterator implements ScoreIterator {

  private final LengthsIterator lengths;
  private final CountIterator counts;
  private final NodeParameters np;
  // parameter :
  private final double c;
  // collectionStats and constants
  private final double averageDocumentLength;
  private final double nodeFrequency;
  private final double documentCount;
  private final double REC_LOG_2_OF_E;

  public PL2ScoringIterator(NodeParameters np, LengthsIterator lengths, CountIterator counts) {
    super(counts);
    this.np = np;
    this.counts = counts;
    this.lengths = lengths;

    c = np.get("c", 1.0);
    averageDocumentLength = (double) np.getLong("collectionLength") / (double) np.getLong("documentCount");
    nodeFrequency = (double) np.getLong("nodeFrequency");
    documentCount = (double) np.getLong("documentCount");
    REC_LOG_2_OF_E = 1.0 / Math.log(2.0); // also equivalent to log_2(e)
  }

  @Override
  public void syncTo(long identifier) throws IOException {
    super.syncTo(identifier);
    lengths.syncTo(identifier);
  }

  @Override
  public double score(ScoringContext cx) {
    double tf = counts.count(cx);
    double docLength = lengths.length(cx);
    
    if (tf == 0) {
      return 0.0;
    }

    double TF = tf * log2(1.0 + (c * averageDocumentLength) / docLength);
    double NORM = 1.0 / (TF + 1.0);
    double f = nodeFrequency / documentCount;

    double score = NORM
            * (TF * log2(1.0 / f)
            + f * REC_LOG_2_OF_E
            + 0.5 * log2(2.0 * Math.PI * TF)
            + TF * (log2(TF) - REC_LOG_2_OF_E));

    // this makes max-Score work
    if (score < 0) {
      score = 0.0;
    }

    return score;
  }

  @Override
  public double maximumScore() {
    return Double.POSITIVE_INFINITY;
  }

  @Override
  public double minimumScore() {
    return Double.NEGATIVE_INFINITY;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = Double.toString(score(c));
    List<AnnotatedNode> children = new ArrayList<>();
    children.add(this.lengths.getAnnotatedNode(c));
    children.add(this.counts.getAnnotatedNode(c));

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  private double log2(double value) {
    return Math.log(value) / Utility.log2;
  }
}

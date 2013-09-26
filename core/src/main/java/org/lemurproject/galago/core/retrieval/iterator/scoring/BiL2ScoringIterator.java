/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.TransformIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Implements BiL2 retrieval model from the DFR framework.
 *
 * @author sjh
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount"})
@RequiredParameters(parameters = {"c"})
public class BiL2ScoringIterator extends TransformIterator implements ScoreIterator {

  private final LengthsIterator lengths;
  private final CountIterator counts;
  private final NodeParameters np;
  // parameter :
  private final double c;
  // collectionStats and constants
  private final double averageDocumentLength;

  public BiL2ScoringIterator(NodeParameters np, LengthsIterator lengths, CountIterator counts) {
    super(counts);
    this.np = np;
    this.counts = counts;
    this.lengths = lengths;

    c = np.get("c", 1.0);
    averageDocumentLength = (double) np.getLong("collectionLength") / (double) np.getLong("documentCount");
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
    
    // if tf or docLength are very small - we can output NaN - instead return 0.0.
    if (tf <= 0 || docLength <= 1.0) {
      return 0.0;
    }

    double TFN = tf * log2(1.0 + (c * averageDocumentLength) / docLength);

    // if this number is negative -> NaN, so let's skip it.
    if(docLength - 1.0 - TFN <= 0){
      return 0.0;
    }
    
    double NORM = 1.0 / (TFN + 1.0);
    double PP = 1.0 / (docLength - 1.0);
    
    double score = NORM
            * (-logFactorial(docLength - 1)
            + logFactorial(TFN)
            + logFactorial(docLength - 1 - TFN)
            - (tf * log2(PP))
            - ((docLength - 1 - TFN) * log2(1 - PP)));
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
    boolean atCandidate = hasMatch(c.document);
    String returnValue = Double.toString(score(c));
    List<AnnotatedNode> children = new ArrayList();
    children.add(this.lengths.getAnnotatedNode(c));
    children.add(this.counts.getAnnotatedNode(c));

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  private double log2(double value) {
    return Math.log(value) / Utility.log2;
  }

  /**
   * Using Sterling's approximation.
   */
  private double logFactorial(double value) {
    return value * Math.log(value) - value + 1.0;
  }
}

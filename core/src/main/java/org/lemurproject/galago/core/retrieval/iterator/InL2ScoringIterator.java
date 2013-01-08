/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Implements InL2 retrieval model from the DFR framework.
 *
 * @author sjh
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount", "nodeDocumentCount", "maximumCount"})
@RequiredParameters(parameters = {"c"})
public class InL2ScoringIterator extends TransformIterator implements MovableScoreIterator {

  private final MovableLengthsIterator lengths;
  private final MovableCountIterator counts;
  private final NodeParameters np;
  // parameter :
  private final double c;
  // collectionStats and constants
  private final double averageDocumentLength;
  private final double nodeDocumentCount;
  private final double documentCount;

  public InL2ScoringIterator(NodeParameters np, MovableLengthsIterator lengths, MovableCountIterator counts) {
    super(counts);
    this.np = np;
    this.counts = counts;
    this.lengths = lengths;

    c = np.get("c", 1.0);
    averageDocumentLength = (double) np.getLong("collectionLength") / (double) np.getLong("documentCount");
    nodeDocumentCount = (double) np.getLong("nodeDocumentCount");
    documentCount = (double) np.getLong("documentCount");
  }

  @Override
  public void syncTo(int identifier) throws IOException {
    super.syncTo(identifier);
    lengths.syncTo(identifier);
  }

  @Override
  public double score() {
    double tf = counts.count();
    if (tf == 0) {
      return 0;
    }

    double docLength = lengths.getCurrentLength();
    double TFN = tf * log2(1.0 + (c * averageDocumentLength) / docLength);
    double NORM = 1.0 / (TFN + 1.0);
    
    double score = NORM * TFN
            * log2( (documentCount + 1.0) / (nodeDocumentCount + 0.5) );
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
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Double.toString(score());
    List<AnnotatedNode> children = new ArrayList();
    children.add(this.lengths.getAnnotatedNode());
    children.add(this.counts.getAnnotatedNode());

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public void setContext(ScoringContext context) {
    super.setContext(context);
    lengths.setContext(context);
  }

  private double log2(double value) {
    return Math.log(value) / Utility.log2;
  }  
}

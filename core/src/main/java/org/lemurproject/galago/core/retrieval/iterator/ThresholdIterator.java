// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * #threshold: raw=[-]x.xx ( PriorReader ScoreIterator ) #threshold: prob=0.xx (
 * PriorReader ScoreIterator ) #threshold: logprob=-x.xx ( PriorReader
 * ScoreIterator )
 *
 * @author sjh
 */
public class ThresholdIterator extends TransformIterator implements IndicatorIterator {

  double threshold;
  ScoreIterator scoreIterator;

  public ThresholdIterator(NodeParameters parameters, ScoreIterator scorer) {
    super(scorer);
    this.scoreIterator = scorer;

    if (parameters.containsKey("raw")) {
      this.threshold = parameters.getDouble("raw");

    } else if (parameters.containsKey("prob")) {
      this.threshold = Math.log(parameters.getDouble("prob"));
      assert this.threshold < 0;

    } else if (parameters.containsKey("logprob")) {
      this.threshold = parameters.getDouble("logprob");
      assert this.threshold < 0;

    } else {
      throw new RuntimeException("#threshold operator requires a thresholding parameter: [raw|prob|logprob]");
    }
  }

  @Override
  public boolean indicator(ScoringContext c) {
    return (scoreIterator.score(c) >= threshold);
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = Boolean.toString(indicator(c));
    List<AnnotatedNode> children = Collections.singletonList(this.iterator.getAnnotatedNode(c));

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}

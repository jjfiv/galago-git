// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * #threshold: raw=[-]x.xx ( PriorReader ScoreIterator ) 
 * #threshold: prob=0.xx ( PriorReader ScoreIterator ) 
 * #threshold: logprob=-x.xx ( PriorReader ScoreIterator ) 
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
      this.threshold = parameters.getDouble("prob");
      assert this.threshold < 0;

    } else if (parameters.containsKey("logprob")) {
      this.threshold = parameters.getDouble("logprob");
      assert this.threshold < 0;

    } else {
      throw new RuntimeException("#threshold operator requires a thresholding parameter: [raw|prob|logprob]");
    }
  }

  /** note that this indicator may depend on the scoring context! **/
  @Override
  public boolean indicator(long identifier) {
    return (scoreIterator.score() >= threshold);
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Boolean.toString(indicator(this.context.document));
    List<AnnotatedNode> children = Collections.singletonList(this.iterator.getAnnotatedNode());

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}

/*
 * BSD License (http://www.galagosearch.org/license)

 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * Not implemented to work over raw counts because it shouldn't blindly be
 * applied to counts - doing that can result in -Infinity scores everywhere.
 * Therefore, this applies to scores, meaning to you had to make a conscious
 * decision to pass a raw count up, and if things go wrong, it's your fault.
 *
 * @author irmarc
 */
public class LogarithmIterator extends TransformIterator implements ScoreIterator {
  NodeParameters np;
  ScoreIterator scorer;

  public LogarithmIterator(NodeParameters params, ScoreIterator svi) {
    super(svi);
    this.np = params;
    scorer = svi;
    context = null;
  }

  @Override
  public double score() {
    return Math.log(scorer.score());
  }

  @Override
  public double maximumScore() {
    return Math.log(scorer.maximumScore());
  }

  @Override
  public double minimumScore() {
    return Math.log(scorer.minimumScore());
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Double.toString(score());
    List<AnnotatedNode> children = Collections.singletonList(this.iterator.getAnnotatedNode());

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }  
}

/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.TransformIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeFrequency", "documentCount"})
public class DFRScoringIterator extends TransformIterator implements ScoreIterator {

  double lambda;
  double qfratio;
  ScoreIterator scorer;
  NodeParameters p;

  public DFRScoringIterator(NodeParameters parameters, ScoreIterator iterator)
          throws IOException {
    super(iterator);
    scorer = iterator;

    // Set the qf ratio
    int qfmax = (int) parameters.get("qfmax", 1);
    int qf = (int) parameters.get("qf", 1);
    qfratio = (qf + 0.0) / (qfmax + 0.0);

    // Set the lambda
    long termFrequency = parameters.getLong("nodeFrequency");
    long documentCount = parameters.getLong("documentCount");
    lambda = (termFrequency + 0.0) / (documentCount + 0.0);
    p = parameters;
  }

  private double transform(double ts) {
    double f1 = ts * Math.log(ts / lambda) / Utility.log2;
    double f2 = (lambda - ts) * Utility.loge_base2;
    double f3 = 0.5 * Math.log(2 * Math.PI * ts) / Utility.log2;
    double risk = 1.0 / (ts + 1.0);
    return qfratio * risk * (f1 + f2 + f3);
  }

  @Override
  public double score(ScoringContext c) {
    double tscore = scorer.score(c);
    double transformedScore = transform(tscore);
    return transformedScore;
  }

  @Override
  public double maximumScore() {
    return transform(scorer.maximumScore());
  }

  @Override
  public double minimumScore() {
    return transform(scorer.minimumScore());
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = p.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c.document);
    String returnValue = Double.toString(score(c));
    List<AnnotatedNode> children = new ArrayList();
    children.add(scorer.getAnnotatedNode(c));
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}

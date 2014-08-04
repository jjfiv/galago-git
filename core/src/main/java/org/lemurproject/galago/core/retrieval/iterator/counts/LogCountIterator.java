package org.lemurproject.galago.core.retrieval.iterator.counts;

import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.TransformIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 * @author jfoley
 */
public class LogCountIterator extends TransformIterator implements ScoreIterator {
  CountIterator counts;
  public LogCountIterator(NodeParameters parameters, CountIterator iterator) {
    super(iterator);
    this.counts = iterator;
  }

  @Override
  public double score(ScoringContext c) {
    // prevent zeroes, and then log
    int count = counts.count(c);
    if(count <= 0) return Double.NEGATIVE_INFINITY;
    return Math.log(count);
  }

  @Override
  public double maximumScore() {
    return Math.log(Integer.MAX_VALUE);
  }

  @Override
  public double minimumScore() {
    return Double.NEGATIVE_INFINITY;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext sc) throws IOException {
    return counts.getAnnotatedNode(sc);
  }
}

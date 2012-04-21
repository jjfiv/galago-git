// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 *
 * @author irmarc
 */
public class BM25FCombinationIterator extends ScoreCombinationIterator {

  double K = -1;
  double[] idfs;

  public BM25FCombinationIterator(NodeParameters parameters,
          MovableScoreIterator[] childIterators) {
    super(parameters, childIterators);
    K = parameters.getDouble("K");
    idfs = new double[iterators.length];
    for (int i = 0; i < idfs.length; i++) {
      idfs[i] = parameters.getDouble("idf" + i);
    }
  }

  @Override
  public double score() {
    double total = 0;

    for (int i = 0; i < scoreIterators.length; i++) {
      double score = scoreIterators[i].score();
      // the second iterator here is the idf iterator - well, it should be
      total += (weights[i] * score) / (K + score) * idfs[i];
    }
    return total;
  }

  @Override
  public double minimumScore() {
    double min = 0;
    double score;
    for (int i = 0; i < scoreIterators.length; i++) {
      score = scoreIterators[i].minimumScore();
      min += (weights[i] * score) / (K + score) * idfs[i];
    }
    return min;
  }

  @Override
  public double maximumScore() {
    double max = 0;
    double score;
    for (int i = 0; i < scoreIterators.length; i++) {
      score = scoreIterators[i].maximumScore();
      max += (weights[i] * score) / (K + score) * idfs[i];
    }
    return max;
  }
}

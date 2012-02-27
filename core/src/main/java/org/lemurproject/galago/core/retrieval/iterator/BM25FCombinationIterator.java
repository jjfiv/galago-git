// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.iterator;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.lemurproject.galago.core.retrieval.processing.FieldDeltaScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
public class BM25FCombinationIterator extends ScoreCombinationIterator {

  double K;

  public BM25FCombinationIterator(Parameters globalParams, NodeParameters parameters,
          MovableScoreIterator[] childIterators) {
    super(globalParams, parameters, childIterators);
    K = parameters.getDouble("K");
  }

  @Override
  public double score() {
    double total = 0;

    for (int i = 0; i < scoreIterators.length; i += 2) {
      double score = scoreIterators[i].score();
      // the second iterator here is the idf iterator - well, it should be
      total += (weights[i] * score) / (K + score) * scoreIterators[i + 1].score();
    }
    return total;
  }

  @Override
  public double score(ScoringContext dc) {
    double total = 0;
    for (int i = 0; i < scoreIterators.length; i += 2) {
      double score = scoreIterators[i].score(dc);
      total += (weights[i] * score) / (K + score) * scoreIterators[i + 1].score(dc);
    }
    return total;
  }

  // Different score combination mechanism, that uses potentials
  // Quits as soon as it is told scoring is done.
  public void score(FieldDeltaScoringContext ctx) {
    ctx.runningScore = ctx.startingPotential;
    ctx.stillScoring = true;
    for (int i = 0; i < iterators.length; i += 2) {
      ((ScoreCombinationIterator) iterators[i]).score(ctx);
      if (!ctx.stillScoring) {
        return;
      }
    }
  }

  @Override
  public double minimumScore() {
    double min = 0;
    double score;
    for (int i = 0; i < scoreIterators.length; i += 2) {
      score = scoreIterators[i].minimumScore();
      min += (weights[i] * score) / (K + score) * scoreIterators[i + 1].score();
    }
    return min;
  }

  @Override
  public double maximumScore() {
    double max = 0;
    double score;
    for (int i = 0; i < scoreIterators.length; i += 2) {
      score = scoreIterators[i].maximumScore();
      max += (weights[i] * score) / (K + score) * scoreIterators[i + 1].maximumScore();
    }
    return max;
  }

  /**
   * This version sets the maximum potential and the K global parameter
   * for BM25F into the context, so all lower nodes have access to those
   * variables (which are global)
   * @param ctx
   */
  @Override
  public void setContext(ScoringContext ctx) {
    if (ctx instanceof FieldDeltaScoringContext) {
      FieldDeltaScoringContext pctx = (FieldDeltaScoringContext) ctx;
      BM25FieldScoringIterator.K = this.K;
      pctx.startingPotential = this.maximumScore();

      // Make sure all scorers have their idfs and parentIdx variables
      // set properly. We also set weight b/c calculating it doesn't depend
      // the values being set below
      TObjectDoubleHashMap idfs = new TObjectDoubleHashMap();
      TObjectIntHashMap idxes = new TObjectIntHashMap();
      pctx.startingPotentials = new double[scoreIterators.length/2];
      pctx.potentials = new double[pctx.startingPotentials.length];
      for (int i = 0; i < scoreIterators.length; i +=2) {
        pctx.startingPotentials[i/2] = scoreIterators[i].maximumScore() + this.K;
        idfs.put(iterators[i], scoreIterators[i+1].maximumScore());
        idxes.put(iterators[i], i/2);
      }
      
      // Set idf and parentIdx
      for (DeltaScoringIterator it : pctx.scorers) {
        BM25FieldScoringIterator scorer = (BM25FieldScoringIterator) it;
        scorer.idf = idfs.get(scorer.parent);
        scorer.parentIdx = idxes.get(scorer.parent);
      }
    }
  }
}

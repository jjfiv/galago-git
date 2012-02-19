// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.iterator;

import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectIntHashMap;
import org.galagosearch.core.retrieval.query.NodeParameters;
import org.galagosearch.core.retrieval.structured.PotentialsContext;
import org.galagosearch.core.retrieval.structured.ScoringContext;
import org.galagosearch.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
public class BM25FCombinationIterator extends ScoreCombinationIterator {

  double K;

  public BM25FCombinationIterator(Parameters globalParams, NodeParameters parameters,
          ScoreValueIterator[] childIterators) {
    super(globalParams, parameters, childIterators);
    K = parameters.getDouble("K");
  }

  @Override
  public double score() {
    double total = 0;

    for (int i = 0; i < iterators.length; i += 2) {
      double score = iterators[i].score();
      // the second iterator here is the idf iterator - well, it should be
      total += (weights[i] * score) / (K + score) * iterators[i + 1].score();
    }
    return total;
  }

  @Override
  public double score(ScoringContext dc) {
    double total = 0;
    for (int i = 0; i < iterators.length; i += 2) {
      double score = iterators[i].score(dc);
      total += (weights[i] * score) / (K + score) * iterators[i + 1].score(dc);
    }
    return total;
  }

  // Different score combination mechanism, that uses potentials
  // Quits as soon as it is told scoring is done.
  public void score(PotentialsContext ctx) {
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
    for (int i = 0; i < iterators.length; i += 2) {
      score = iterators[i].minimumScore();
      min += (weights[i] * score) / (K + score) * iterators[i + 1].score();
    }
    return min;
  }

  @Override
  public double maximumScore() {
    double max = 0;
    double score;
    for (int i = 0; i < iterators.length; i += 2) {
      score = iterators[i].maximumScore();
      max += (weights[i] * score) / (K + score) * iterators[i + 1].maximumScore();
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
    //super.setContext(ctx);
    if (ctx instanceof PotentialsContext) {
      PotentialsContext pctx = (PotentialsContext) ctx;
      pctx.K = this.K;
      pctx.startingPotential = this.maximumScore();
      //System.err.printf("K=%f, startingPot=%f\n", pctx.K, pctx.startingPotential);

      // Make sure all scorers have their idfs and parentIdx variables
      // set properly. We also set weight b/c calculating it doesn't depend
      // the values being set below
      TObjectDoubleHashMap idfs = new TObjectDoubleHashMap();
      TObjectIntHashMap idxes = new TObjectIntHashMap();
      pctx.startingFieldSums = new double[iterators.length/2];
      pctx.currentFieldSums = new double[pctx.startingFieldSums.length];
      for (int i = 0; i < iterators.length; i +=2) {
        pctx.startingFieldSums[i/2] = iterators[i].maximumScore() + pctx.K;
	//System.err.printf("(%d) startingFS=%f\n", i/2, pctx.startingFieldSums[i/2]);
        idfs.put(iterators[i], iterators[i+1].maximumScore());
        idxes.put(iterators[i], i/2);
      }
      
      // Set idf and parentIdx
      for (BM25FieldScoringIterator scorer : pctx.scorers) {
        scorer.idf = idfs.get(scorer.parent);
        scorer.parentIdx = idxes.get(scorer.parent);
      }
      
     
    }
  }
}

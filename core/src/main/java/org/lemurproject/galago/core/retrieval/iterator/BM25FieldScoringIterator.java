/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.disk.PositionIndexReader;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.BM25FieldScorer;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * A ScoringIterator that makes use of the BM25FieldScorer function
 * for converting a count into a score.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeDocumentCount", "collectionLength", "documentCount"})
public class BM25FieldScoringIterator extends ScoringFunctionIterator {

  String partName;
  double max;
  public ScoreCombinationIterator parent;
  public int parentIdx;
  public double weight;
  public double idf;

  public BM25FieldScoringIterator(Parameters globalParams, NodeParameters p, CountValueIterator it)
          throws IOException {
    super(it, new BM25FieldScorer(globalParams, p, it));
    partName = p.getString("lengths");
    if (it instanceof PositionIndexReader.TermExtentIterator) {
      PositionIndexReader.TermExtentIterator maxIter = (PositionIndexReader.TermExtentIterator) it;
      max = function.score(maxIter.maximumCount(), maxIter.maximumCount());
    } else {
      max = 0;  // Means we have a null extent iterator
    }
  }  
  
  @Override
  public double score() {
    if (context instanceof FieldScoringContext) {
      int count = 0;

      if (iterator.currentCandidate() == context.document) {
        count = ((CountIterator) iterator).count();
      }
      double score = function.score(count, ((FieldScoringContext) context).lengths.get(partName));
      return score;
    } else {
      return super.score();
    }
  }

  // Use this to score for potentials, which is more of an "adjustment" than just scoring.
  public void score(PotentialsContext ctx) {
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }

    double s = function.score(count, ((FieldScoringContext) context).lengths.get(partName));
    double diff = weight * (s - max);
    double numerator = idf * ctx.K * diff;
    double fieldSum = ctx.currentFieldSums[parentIdx];
    double denominator = fieldSum * (fieldSum + diff);
    /*
    System.err.printf("%s updating (%d, %d): max=%f, score=%f, idf=%f, diff=%f, weight=%f, oldVal=%f, newVal=%f\n",
		      this.toString(), parentIdx, currentCandidate(), max, s, idf, diff, weight, ctx.currentFieldSums[parentIdx], fieldSum);
    */
    ctx.runningScore += (numerator / denominator);
    ctx.currentFieldSums[parentIdx] += diff;
  }

  public void maximumAdjustment(PotentialsContext ctx) {
    double diff = weight * (0 - max);
    double numerator = idf * ctx.K * diff;
    double fieldSum = ctx.currentFieldSums[parentIdx];
    double denominator = fieldSum * (fieldSum + diff);
    /*
    System.err.printf("%s updating (%d, %d): max=%f, min=%f, idf=%f, diff=%f, weight=%f, oldVal=%f, newVal=%f\n",
		      this.toString(), parentIdx, currentCandidate(), max, 0.0, idf, diff, weight, ctx.currentFieldSums[parentIdx], fieldSum);
    */
    ctx.runningScore += (numerator / denominator);
    ctx.currentFieldSums[parentIdx] += diff;
  }
  
  @Override
  public double maximumScore() {
    return max;
  }
  
  @Override
  public double minimumScore() {
    return 0;
  }
}

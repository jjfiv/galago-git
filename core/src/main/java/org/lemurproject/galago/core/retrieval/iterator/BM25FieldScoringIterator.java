// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import gnu.trove.map.hash.TIntDoubleHashMap;
import java.io.IOException;
import org.lemurproject.galago.core.index.disk.PositionIndexReader;
import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.scoring.BM25FieldScorer;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * A ScoringIterator that makes use of the BM25FieldScorer function for
 * converting a count into a score.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeDocumentCount", "collectionLength", "documentCount"})
@RequiredParameters(parameters = {"b"})
public class BM25FieldScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator {

  String partName;
  public ScoreCombinationIterator parent;
  public int parentIdx;
  public double weight;
  public double idf;
  public static double K;

  public BM25FieldScoringIterator(NodeParameters p, MovableCountIterator it)
          throws IOException {
    super(p, it, new BM25FieldScorer(p, it));
    partName = p.getString("lengths");
    weight = p.getDouble("w");
    parentIdx = (int) p.getLong("pIdx");
    idf = p.getDouble("idf");
    K = p.getDouble("K"); // not the most efficient since it's a static, but meh
    max = getMaxTF(p, it);
  }

  public double getWeight() {
    return weight;
  }

  @Override
  public double score() {
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }
    double score = function.score(count, context.getLength(partName));
    return score;
  }

  // Use this to score for potentials, which is more of an "adjustment" than just scoring.
  @Override
  public void deltaScore(int count, int length) {
    DeltaScoringContext ctx = (DeltaScoringContext) context;

    double s = function.score(count, length);
    double diff = weight * (s - max);
    double numerator = idf * K * diff;
    double fieldSum = ctx.potentials[parentIdx];
    double denominator = fieldSum * (fieldSum + diff);

    double inc = numerator / denominator;

    ctx.runningScore += inc;
    ctx.potentials[parentIdx] += diff;
  }

  @Override
  public void deltaScore(int length) {
    DeltaScoringContext ctx = (DeltaScoringContext) context;
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }

    double s = function.score(count, length);
    double diff = weight * (s - max);
    double numerator = idf * K * diff;
    double fieldSum = ctx.potentials[parentIdx];
    double denominator = fieldSum * (fieldSum + diff);

    double inc = numerator / denominator;

    ctx.runningScore += inc;
    ctx.potentials[parentIdx] += diff;
  }

  // Use this to score for potentials, which is more of an "adjustment" than just scoring.
  @Override
  public void deltaScore() {
    DeltaScoringContext ctx = (DeltaScoringContext) context;
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }

    double s = function.score(count, context.getLength(partName));
    double diff = weight * (s - max);
    double numerator = idf * K * diff;
    double fieldSum = ctx.potentials[parentIdx];
    double denominator = fieldSum * (fieldSum + diff);

    double inc = numerator / denominator;

    ctx.runningScore += inc;
    ctx.potentials[parentIdx] += diff;
  }

  public void maximumDifference() {
    DeltaScoringContext ctx = (DeltaScoringContext) context;
    double diff = weight * (0 - max);
    double numerator = idf * K * diff;
    double fieldSum = ctx.potentials[parentIdx];
    double denominator = fieldSum * (fieldSum + diff);

    ctx.runningScore += (numerator / denominator);
    ctx.potentials[parentIdx] += diff;
  }

  public void aggregatePotentials(DeltaScoringContext ctx) {
    TIntDoubleHashMap idfs = new TIntDoubleHashMap();
    for (int i = 0; i < ctx.scorers.size(); i++) {
      BM25FieldScoringIterator it = (BM25FieldScoringIterator) ctx.scorers.get(i);
      ctx.startingPotentials[it.parentIdx] += (it.weight * it.maximumScore());
      idfs.put(it.parentIdx, it.idf);
    }

    for (int i = 0; i < ctx.startingPotentials.length; i++) {
      double num = ctx.startingPotentials[i];
      double den = num + this.K;
      ctx.startingPotential += idfs.get(i) * (num / den);
      ctx.startingPotentials[i] += this.K;
    }
  }

  @Override
  public double minimumScore() {
    return 0;
  }

  @Override
  public void setContext(ScoringContext ctx) {
    super.setContext(ctx);
    if (DeltaScoringContext.class.isAssignableFrom(ctx.getClass())) {
      DeltaScoringContext dctx = (DeltaScoringContext) ctx;
      dctx.scorers.add(this);
    }
  }
}

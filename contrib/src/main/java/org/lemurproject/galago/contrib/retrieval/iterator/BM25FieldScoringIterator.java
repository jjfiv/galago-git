// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreCombinationIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.iterator.scoring.BM25FieldScorer;
import org.lemurproject.galago.core.retrieval.iterator.scoring.ScoringFunction;

/**
 *
 * A ScoringIterator that makes use of the BM25FieldScorer function for
 * converting a count into a score.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeDocumentCount", "collectionLength", "documentCount", "maximumCount"})
@RequiredParameters(parameters = {"b"})
public class BM25FieldScoringIterator extends ScoringFunctionIterator //        implements DeltaScoringIterator {
{

  String partName;
  public ScoreCombinationIterator parent;
  public int parentIdx;
  public double weight;
  public double idf;
  public static double K;
  double max;
  ScoringFunction function;

  public BM25FieldScoringIterator(NodeParameters p, LengthsIterator ls, CountIterator it)
          throws IOException {
    super(p, ls, it);
    function = new BM25FieldScorer(p);
    partName = p.getString("lengths");
    weight = p.getDouble("w");
    parentIdx = (int) p.getLong("pIdx");
    idf = p.getDouble("idf");
    K = p.getDouble("K"); // not the most efficient since it's a static, but meh
    max = p.getLong("maximumCount");
  }

//  @Override
//  public double getWeight() {
//    return weight;
//  }
  @Override
  public double score(ScoringContext c) {
    int count = (countIterator).count(c);
    double score = function.score(count, lengthsIterator.length(c));
    return score;
  }

//  // Use this to score for potentials, which is more of an "adjustment" than just scoring.
//  @Override
//  public void deltaScore() {
//    EarlyTerminationScoringContext ctx = (EarlyTerminationScoringContext) context;
//    int count = 0;
//
//    if (iterator.currentCandidate() == context.document) {
//      count = ((CountIterator) iterator).count(context);
//    }
//
//    double s = function.score(count, lengthsIterator.length(context));
//    double diff = weight * (s - max);
//    double numerator = idf * K * diff;
//    double fieldSum = ctx.potentials[parentIdx];
//    double denominator = fieldSum * (fieldSum + diff);
//
//    double inc = numerator / denominator;
//
//    ctx.runningScore += inc;
//    ctx.potentials[parentIdx] += diff;
//  }
//
//  @Override
//  public void maximumDifference() {
//    EarlyTerminationScoringContext ctx = (EarlyTerminationScoringContext) context;
//    double diff = weight * (0 - max);
//    double numerator = idf * K * diff;
//    double fieldSum = ctx.potentials[parentIdx];
//    double denominator = fieldSum * (fieldSum + diff);
//
//    ctx.runningScore += (numerator / denominator);
//    ctx.potentials[parentIdx] += diff;
//  }
//
//  @Override
//  public void aggregatePotentials(EarlyTerminationScoringContext ctx) {
//    TIntDoubleHashMap idfs = new TIntDoubleHashMap();
//    for (int i = 0; i < ctx.scorers.size(); i++) {
//      BM25FieldScoringIterator it = (BM25FieldScoringIterator) ctx.scorers.get(i);
//      ctx.startingPotentials[it.parentIdx] += (it.weight * it.maximumScore());
//      idfs.put(it.parentIdx, it.idf);
//    }
//
//    for (int i = 0; i < ctx.startingPotentials.length; i++) {
//      double num = ctx.startingPotentials[i];
//      double den = num + this.K;
//      ctx.startingPotential += idfs.get(i) * (num / den);
//      ctx.startingPotentials[i] += this.K;
//    }
//  }
  @Override
  public double minimumScore() {
    return 0;
  }
//  @Override
//  public double startingPotential() {
//    return 0.0;
//  }
}

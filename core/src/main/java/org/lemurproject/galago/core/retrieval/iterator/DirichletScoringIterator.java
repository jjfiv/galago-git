// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.DirichletScorer;

/**
 *
 * A ScoringIterator that makes use of the DirichletScorer function for
 * converting a count into a score.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount", "nodeFrequency", "maximumCount"})
@RequiredParameters(parameters = {"mu"})
public class DirichletScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator {

  double weight;
  int parentIdx;
  double min; // min score
  double max; // max tf

  public DirichletScoringIterator(NodeParameters p, LengthsIterator ls, CountIterator it)
          throws IOException {
    super(p, ls, it);
    this.setScoringFunction(new DirichletScorer(p, it));
    weight = p.get("w", 1.0);
    parentIdx = (int) p.get("pIdx", 0);
    max = p.getLong("maximumCount");
    //long collectionLength = p.getLong("collectionLength");    
    //long documentCount = p.getLong("documentCount");
    //int avgDocLength = (int) Math.round((collectionLength + 0.0) / (documentCount + 0.0));
    int avgDocLength = 1200; /// ...UGH
    min = function.score(0, avgDocLength); // Allows for a slightly more conservative "worst-case"
  }

  @Override
  public double minimumScore() {
    return min;
  }
  
  @Override
  public double maximumScore() {
    return max;
  }

  @Override
  public double getWeight() {
    return weight;
  }

  @Override
  public double deltaScore(ScoringContext c) {
    int count = ((CountIterator) iterator).count(c);
    int length = this.lengthsIterator.length(c);
    double diff = weight * (function.score(count, length) - max);
    return diff;
  }

  @Override
  public double maximumDifference() {
    double diff = weight * (min - max);
    return diff;
  }

  @Override
  public double startingPotential() {
    return (max * weight);
  }
}

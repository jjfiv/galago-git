// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.contrib.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.scoring.PL2FieldScorer;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.IOException;

/**
 *
 * A ScoringIterator that makes use of the PL2F scoring function for converting
 * a document field count into a score.
 *
 * In the delta-function form, computes the incremental change when this nodes
 * potential is replaced by the concrete count from a document.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount"})
@RequiredParameters(parameters = {"c"})
public class PL2FieldScoringIterator extends ScoringFunctionIterator {

  String partName;
  double min = 0.0001;
  int parentIdx = -1;
  double weight;
  double beta;
  double max;
  PL2FieldScorer f;

  public PL2FieldScoringIterator(NodeParameters p, LengthsIterator ls, CountIterator it)
          throws IOException {
    super(p, ls, it);
    f = new PL2FieldScorer(p);
    partName = p.getString("lengths");
    weight = p.getDouble("w");
    parentIdx = (int) p.getLong("pIdx");
    long termFrequency = p.getLong("nf");
    long documentCount = p.getLong("dc");
    double lambda = (termFrequency + 0.0) / (documentCount + 0.0);
    beta = Math.log(lambda) / Utility.log2 + (lambda * Utility.loge_base2)
            + ((0.5 * (Math.log(2 * Math.PI) / Utility.log2)) + Utility.loge_base2);
    max = p.getLong("maximumCount");
  }

  @Override
  public double score(ScoringContext c) {
    int count = ((CountIterator) iterator).count(c);
    double score = f.score(count, this.lengthsIterator.length(c));
    score = (score > 0.0) ? score : min; // MY smoothing.
    return score;
  }

  @Override
  public double minimumScore() {
    return min;
  }
}

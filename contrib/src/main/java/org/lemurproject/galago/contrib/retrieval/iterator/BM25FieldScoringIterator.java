// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.scoring.BM25FieldScorer;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 *
 * A ScoringIterator that makes use of the BM25FieldScorer function for
 * converting a count into a score.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeDocumentCount", "collectionLength", "documentCount", "maximumCount"})
@RequiredParameters(parameters = {"b"})
public class BM25FieldScoringIterator extends ScoringFunctionIterator {
  BM25FieldScorer function;

  public BM25FieldScoringIterator(NodeParameters p, LengthsIterator ls, CountIterator it)
          throws IOException {
    super(p, ls, it);
    function = new BM25FieldScorer(p);
  }

  @Override
  public double score(ScoringContext c) {
    int count = (countIterator).count(c);
    double score = function.score(count, lengthsIterator.length(c));
    return score;
  }

}

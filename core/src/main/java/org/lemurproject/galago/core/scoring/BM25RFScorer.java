// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.scoring;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;

/**
 * So, this scoring function is quite nice because it gives a flat score to all
 * requests. It is the responsiblity of the iterator *using* this function to
 * know when to call it (i.e. only for documents in its iteration list),
 * otherwise every identifier in the collection will get a flat boost, changing
 * nothing. The intention is that only the documents in the target term's
 * posting list will get this score increment.
 *
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"documentCount"})
@RequiredParameters(parameters = {"rt", "R", "factor", "ft"})
public class BM25RFScorer implements ScoringFunction {

  double value;

  public BM25RFScorer(NodeParameters parameters, MovableCountIterator iterator) throws IOException {
    int rt = (int) parameters.get("rt", 0);
    int R = (int) parameters.get("R", 0);
    long N = parameters.getLong("documentCount");
    double factor = parameters.get("factor", 0.33D);
    // now get idf
    long ft = 0;
    if (parameters.containsKey("ft")) {
      ft = (int) parameters.get("ft", 0);
    } else {
      ft = iterator.totalEntries();
    }
    assert (ft >= rt); // otherwise they're wrong and/or lying
    double numerator = (rt + 0.5) / (R - rt + 0.5);
    double denominator = (ft - rt + 0.5) / (N - ft - R + rt + 0.5);
    value = factor * Math.log(numerator / denominator);
  }

  /**
   * Returns a constant value, determined by term selection value. See the
   * #TermSelectionValueModel for details.
   *
   * @param count
   * @param length
   * @return
   */
  public double score(int count, int length) {
    return value;
  }
}

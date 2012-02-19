/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.scoring;

import java.io.IOException;
import org.galagosearch.core.retrieval.iterator.CountValueIterator;
import org.galagosearch.core.retrieval.query.NodeParameters;
import org.galagosearch.core.retrieval.structured.RequiredStatistics;
import org.galagosearch.tupleflow.Parameters;

/**
 * Term-dependent-only function. Always returns the idf of the term iterator it was constructed from.
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeDocumentCount", "collectionLength", "documentCount"})
public class InverseDocumentFrequencyScorer implements ScoringFunction {

  double idf;

  public InverseDocumentFrequencyScorer(Parameters globalParams, NodeParameters parameters, CountValueIterator iterator) throws IOException {
    // get idf for the provided term.
    double documentCount = parameters.getLong("documentCount");
    long df = parameters.getLong("nodeDocumentCount");

    if (parameters.get("rsj", false)) {
      idf = Math.log((documentCount - df + 0.5) / (df + 0.5));
    } else {
      idf = Math.log(documentCount / (df + 0.5)); // add a wee bit to keep the denominator from hitting zero
    }
  }

  public double score(int count, int length) {
    return idf;
  }
}

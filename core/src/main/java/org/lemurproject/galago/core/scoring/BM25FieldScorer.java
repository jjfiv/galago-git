// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.scoring;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.CountValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Low-level node use to generate the term-frequency of field f for document d.
 * 
 * Referring "Microsoft Cambridge at TREC-13: Web and HARD Tracks" by Robertson et al.
 * for further details, this is the code that implements the {\bar x}_{d,f,t} formula.
 *
 * Assumptions: collection statistics provided are from a particular field index.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeDocumentCount", "collectionLength", "documentCount"})
public class BM25FieldScorer implements ScoringFunction {

  double b;
  double avgDocLength;

  public BM25FieldScorer(Parameters globalParams, NodeParameters parameters, CountValueIterator iterator) throws IOException {
    b = parameters.get("b", globalParams.get("b", 0.5));

    if (b < 0 || b > 1.0) throw new IllegalArgumentException("b parameter must be between 0 and 1");
    
    long collectionLength = parameters.getLong("collectionLength");
    long documentCount = parameters.getLong("documentCount");
    avgDocLength = (collectionLength + 0.0) / (documentCount + 0.0);

  }

  public double score(int count, int length) {
    double numerator = count;
    double denominator = 1.0 + (b * ( (length / avgDocLength) - 1.0) );
    //System.err.printf("%s: b=%f, l=%d, avgdl=%f, count=%d, score=%f\n", this.toString(), b, length, avgDocLength, count, (numerator/denominator));
    return numerator / denominator;
  }
}

/*
 * BSD License (http://www.galagosearch.org/license)

 */
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;

/**
 *
 * @author irmarc
 * @deprecated 
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount"})
@RequiredParameters(parameters = {"c"})
public class PL2FieldScorer implements ScoringFunction {

    double avgDocLength;
    double c;
    double log2;
    public PL2FieldScorer(NodeParameters parameters) throws IOException {
    c = parameters.get("c", 0.5);

    if (c <= 0) throw new IllegalArgumentException("c parameter must be greater than 0");
    
    long collectionLength = parameters.getLong("collectionLength");
    long documentCount = parameters.getLong("documentCount");
    avgDocLength = (collectionLength + 0.0) / (documentCount + 0.0);
    log2 = Math.log(2);
  }
  
  @Override
  public double score(int count, int length) {
    double factor = Math.log(1 + (c * (avgDocLength / length)))/log2;
    /*
    System.err.printf("scoring: count=%d, length=%d, avgdl=%f, c=%f, factor=%f, score=%f\n",
            count, length, avgDocLength, c, factor, (count*factor));
     * 
     */
    return count * factor;
  }
}

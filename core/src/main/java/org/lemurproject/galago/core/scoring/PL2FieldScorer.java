/*
 * BSD License (http://www.galagosearch.org/license)

 */
package org.lemurproject.galago.core.scoring;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount"})
public class PL2FieldScorer implements ScoringFunction {

    double avgDocLength;
    double c;
    double log2;
    public PL2FieldScorer(Parameters globalParams, NodeParameters parameters, MovableCountIterator iterator) throws IOException {
    c = parameters.get("c", globalParams.get("c", 0.5));

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

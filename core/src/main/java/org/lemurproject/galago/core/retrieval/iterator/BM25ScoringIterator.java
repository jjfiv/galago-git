// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.BM25Scorer;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount","nodeDocumentCount"})
public class BM25ScoringIterator extends ScoringFunctionIterator {
    public BM25ScoringIterator(Parameters globalParams, NodeParameters p, CountValueIterator it)
        throws IOException {
        super(it, new BM25Scorer(globalParams, p, it));
    }

    /**
     * Score is maxed by having count = length, but having both numbers be as high
     * as possible.
     * @return
     */
    public double maximumScore() {
        return function.score(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    /**
     * Minimized by having no occurrences.
     * @return
     */
    public double minimumScore() {
        return function.score(0, Integer.MAX_VALUE);
    }
}

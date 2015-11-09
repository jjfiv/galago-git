// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.index.stats.NodeAggregateIterator;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

import java.io.IOException;

/**
 * This is base interface for all inverted lists that return count information.
 *
 * @author trevor, irmarc, sjh
 */
public interface CountIterator extends BaseIterator, IndicatorIterator {
    /**
     * Returns the number of occurrences of this iterator's term in
     * the current identifier.
     */
    int count(ScoringContext c);

    default NodeStatistics getOrCalculateStatistics() throws IOException {
        if(this instanceof NodeAggregateIterator) {
            return ((NodeAggregateIterator) this).getStatistics();
        }
        return calculateStatistics();
    }

    default NodeStatistics calculateStatistics() throws IOException {
        long startTime = System.currentTimeMillis();
        NodeStatistics s = new NodeStatistics();
        // set up initial values
        s.nodeDocumentCount = 0;
        s.nodeFrequency = 0;
        s.maximumCount = 0;

        this.reset();
        this.forEach((ctx) -> {
            int c = this.count(ctx);
            s.nodeFrequency += c;
            s.maximumCount = Math.max(c, s.maximumCount);
            s.nodeDocumentCount += (c > 0) ? 1 : 0; // positive counting
        });
        this.reset();
        long endTime = System.currentTimeMillis();

        // weigh these statistics in cache by how long they took to compute.
        s.computationCost = endTime - startTime;

        return s;
    }
}

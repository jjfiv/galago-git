// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.query;

/**
 * The types of query that may be executed by a retrieval. 
 * 
 * RANKED indicates that the top node in the query tree must resolve to a ScoreIterator,
 * and the results are type-bounded as ScoredDocuments.
 *
 * BOOLEAN is a set retrieval, therefore the root node must resolve to an AbstractIndicator.
 * Results are still ScoredDocuments, but they are not ordered.
 *
 * COUNT returns a long, and the root node must resolve to a CountIterator.
 *
 * @author irmarc
 */
public enum QueryType {
    RANKED, BOOLEAN, COUNT, UNKNOWN
}

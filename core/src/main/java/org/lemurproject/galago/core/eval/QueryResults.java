/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval;

import java.util.List;
import org.lemurproject.galago.core.retrieval.ScoredDocument;

/**
 * Stores a list of returned ScoredDocuments for a query.
 *
 * @author sjh
 */
public class QueryResults {

  private String query;
  private List<ScoredDocument> rankedList;

  public QueryResults(String query, List<ScoredDocument> rankedList) {
    this.query = query;
    this.rankedList = rankedList;
  }

  public Iterable<ScoredDocument> getIterator() {
    return rankedList;
  }

  public int size() {
    return rankedList.size();
  }
}

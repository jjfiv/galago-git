/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval;

import javax.annotation.Nonnull;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * Stores a list of returned ScoredDocuments for a query.
 *
 * @author sjh
 */
public class QueryResults extends AbstractList<EvalDoc> {

  private String query;
  private List<EvalDoc> rankedList;

  public QueryResults(@Nonnull List<? extends EvalDoc> rankedList) {
    this("unknown-id", rankedList);
  }
  public QueryResults(String query, @Nonnull List<? extends EvalDoc> rankedList) {
    this.query = query;
    this.rankedList = new ArrayList<>(rankedList);
  }

  @Nonnull
  public Iterable<EvalDoc> getIterator() {
    return rankedList;
  }

  @Override
  public int size() {
    return rankedList.size();
  }

  @Override
  public EvalDoc get(int index) {
    return rankedList.get(index);
  }
}

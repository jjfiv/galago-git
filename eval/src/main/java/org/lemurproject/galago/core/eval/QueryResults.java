/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * Stores a list of returned ScoredDocuments for a query.
 *
 * @author sjh
 */
public class QueryResults extends AbstractList<EvalDoc> implements Comparable<QueryResults> {

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

  @Nullable
  public EvalDoc find(String docId) {
    for (EvalDoc evalDoc : this) {
      if(evalDoc.getName().equals(docId)) {
        return evalDoc;
      }
    }
    return null;
  }

  /**
   * Returns the score for the given document id or a default value.
   * @param docId
   * @param backup
   * @return
   */
  public double findScore(String docId, double backup) {
    for (EvalDoc evalDoc : this) {
      if(evalDoc.getName().equals(docId)) {
        return evalDoc.getScore();
      }
    }
    return backup;
  }

  @Override
  public int compareTo(QueryResults o) {
    return this.query.compareTo(o.query);
  }

  public void outputTrecrun(PrintWriter train, String systemName) {
    for (EvalDoc d : this) {
      train.printf("%s Q0 %s %d %10.8f %s\n", query, d.getName(), d.getRank(), d.getScore(), systemName);
    }
  }

  /**
   * @return the qid of these results, if available.
   */
  public String getQuery() {
    return query;
  }

}

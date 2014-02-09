/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.lemurproject.galago.tupleflow.util.WrappedMap;

/**
 * This class store a relevance judgment of documents for a specific query.
 * Relevance is represented by an integer - positive indicates that the document
 * is relevant - zero indicates that the document is not relevant - negative
 * indicates that the document is detrimental to results (irrelevant)
 *
 * @author sjh, trevor
 */
public class QueryJudgments extends WrappedMap<String, Integer> {

  // each instance of QueryJudgments correspond to some query
  private String queryName;

  /**
   * Initialize as a builder, with no prior Map
   *
   * @param queryName
   */
  public QueryJudgments(String queryName) {
    this(queryName, new TreeMap());
  }

  /**
   * Initialize from a Map
   *
   * @param queryName
   * @param judgments
   */
  public QueryJudgments(String queryName, Map<String, Integer> judgments) {
    super(judgments);
    this.queryName = queryName;
  }

  public void add(String documentName, int judgment) {
    if (containsKey(documentName)) {
      assert (get(documentName) == judgment) : "Query: " + this.queryName + " Document :" + documentName + " has been double judged, and the judgments to not match.";
    } else { // ! judgments.contains(documentName)
      put(documentName, judgment);
    }
  }

  /**
   * Anything not present and positive is not "relevant".
   * @param documentName
   * @return 
   */
  public boolean isRelevant(String documentName) {
    if (containsKey(documentName)) {
      return (get(documentName) > 0);
    } else {
      return false;
    }
  }

  /**
   * Note that the semantics of this appears to assume that if the document is
   * missing, it is not explicitly irrelevant. So even though it returns a
   * boolean, this coupled with (@see isRelevant) allows you to determine the
   * ternary status of good, unjudged or bad.
   *
   * @see isRelevant
   * @param documentName
   * @return
   */
  public boolean isNonRelevant(String documentName) {
    if (containsKey(documentName)) {
      return (get(documentName) <= 0);
    } else {
      return false;
    }
  }

  public boolean isJudged(String documentName) {
    return containsKey(documentName);
  }

  /**
   * Calculates the number of relevant documents.
   * @return 
   */
  public int getRelevantJudgmentCount() {
    int rel_count = 0;
    for (int judgment : values()) {
      if (judgment > 0) {
        rel_count++;
      }
    }
    return rel_count;
  }

  /**
   * Calculate the number of non-relevant documents.
   * @return 
   */
  public int getNonRelevantJudgmentCount() {
    int non_rel_count = 0;
    for (int judgment : values()) {
      if (judgment <= 0) {
        non_rel_count++;
      }
    }
    return non_rel_count;
  }

  public int get(String documentName) {
    if (containsKey(documentName)) {
      return this.wrapped.get(documentName);
    } else {
      return 0;
    }
  }

  public Set<String> getDocumentSet() {
    return keySet();
  }
}

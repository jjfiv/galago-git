/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval;

import java.util.Set;
import java.util.TreeMap;

/**
 * This class store a relevance judgment of documents for a specific query.
 * Relevance is represented by an integer
 *  - positive indicates that the document is relevant
 *  - zero indicates that the document is not relevant
 *  - negative indicates that the document is detrimental to results (irrelevant)
 *
 * @author sjh, trevor
 */
public class QueryJudgments {

  // each instance of QueryJudgments correspond to some query
  private String queryName;
  /** mapping from documentNumber to judgment value
   * where positive values mean relevant, 
   * where negative values mean irrelevant, 
   * and zero means not relevant. 
   */
  private TreeMap<String, Integer> judgments;
  // global stats
  private int relevant_judgment_count;
  private int nonrelevant_judgment_count;

  public QueryJudgments(String queryName) {
    this.queryName = queryName;
    this.judgments = new TreeMap();
    this.relevant_judgment_count = 0;
    this.nonrelevant_judgment_count = 0;
  }

  public void add(String documentName, int judgment) {
    if (judgments.containsKey(documentName)) {
      assert (judgments.get(documentName) == judgment) : "Query: " + this.queryName + " Document :" + documentName + " has been double judged, and the judgments to not match.";
    } else { // ! judgments.contains(documentName)
      judgments.put(documentName, judgment);
      if (judgment > 0) {
        relevant_judgment_count++;
      } else if (judgment <= 0) {
        nonrelevant_judgment_count++;
      }
    }
  }

  public boolean isRelevant(String documentName) {
    if (this.judgments.containsKey(documentName)) {
      return (this.judgments.get(documentName) > 0);
    } else {
      return false;
    }
  }

  public boolean isNonRelevant(String documentName) {
    if (this.judgments.containsKey(documentName)) {
      return (this.judgments.get(documentName) <= 0);
    } else {
      return false;
    }
  }
  
  public int getRelevantJudgmentCount() {
    return this.relevant_judgment_count;
  }

  public int getNonRelevantJudgmentCount() {
    return this.nonrelevant_judgment_count;
  }

  public int get(String documentName) {
    if (this.judgments.containsKey(documentName)) {
      return this.judgments.get(documentName);
    } else {
      return 0;
    }
  }

  public Iterable<Integer> getIterator() {
    return judgments.values();
  }
  
  public Set<String> getDocumentSet() {
    return judgments.keySet();
  }

  public int size() {
    return judgments.size();
  }
}

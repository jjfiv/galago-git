/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval;

import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author sjh
 */
public class QuerySetEvaluation {
  
  private Map<String, Double> querySetEvaluation;
  
  public QuerySetEvaluation(){
    this.querySetEvaluation = new TreeMap();
  }
  
  public void add(String query, double evaluation){
    this.querySetEvaluation.put(query, evaluation);
  }

  public Iterable<String> getIterator(){
    return querySetEvaluation.keySet();
  }
  
  public double get(String query){
    return querySetEvaluation.get(query);
  }
  
  public int size(){
    return querySetEvaluation.size();
  }
}

/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.Node;

/**
 * Base results class Wrapper for a list of results, and provides some utility
 * functions for merging several results
 *
 * Can be sub-classed in
 *
 * @author sjh
 */
public class Results implements Serializable{

  public Node inputQuery;
  public List<ScoredDocument> scoredDocuments;

  // empty construction -- a
  public Results() {
  }

  public Results(Node query) {
    this.inputQuery = query;
    scoredDocuments = new ArrayList();
  }

  public Results(Node query, List<ScoredDocument> scoredDocuments) {
    this.inputQuery = query;
    this.scoredDocuments = scoredDocuments;
  }

  public void addBest(List<Results> results, int requested) {
    // create new results, and merge in sorted order
  }
}

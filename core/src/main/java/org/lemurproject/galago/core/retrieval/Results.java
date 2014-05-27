/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import org.lemurproject.galago.core.retrieval.query.Node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Base results class Wrapper for a list of results, and provides some utility
 * functions for merging several results
 *
 * @author sjh
 */
public class Results implements Serializable {

  public Node inputQuery;
  public List<ScoredDocument> scoredDocuments;
  public Class<? extends ProcessingModel> processingModel;

  // empty construction -- a
  public Results() {
    this.scoredDocuments = new ArrayList<ScoredDocument>();
  }

  public Results(Node query) {
    this.inputQuery = query;
    this.scoredDocuments = new ArrayList<ScoredDocument>();
  }

  public Results(Node query, List<ScoredDocument> scoredDocuments) {
    this.inputQuery = query;
    this.scoredDocuments = scoredDocuments;
  }

  /**
   * When performing passage retrieval, it's probably convenient to have this method.
   * @return a list of results, cast to ScoredPassages.
   */
  public List<ScoredPassage> asPassages() {
    ArrayList<ScoredPassage> passages = new ArrayList<ScoredPassage>();
    for(ScoredDocument doc : scoredDocuments) {
      assert(doc instanceof ScoredPassage);
      passages.add((ScoredPassage) doc);
    }
    return passages;
  }

  public void addBest(List<Results> results, int requested) {
    // TODO create new results, and merge in sorted order
    throw new UnsupportedOperationException();
  }
}

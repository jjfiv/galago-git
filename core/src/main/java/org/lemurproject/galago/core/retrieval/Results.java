/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import org.lemurproject.galago.core.retrieval.query.Node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    this.scoredDocuments = new ArrayList<>();
  }

  public Results(Node query) {
    this.inputQuery = query;
    this.scoredDocuments = new ArrayList<>();
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
    ArrayList<ScoredPassage> passages = new ArrayList<>();
    for(ScoredDocument doc : scoredDocuments) {
      assert(doc instanceof ScoredPassage);
      passages.add((ScoredPassage) doc);
    }
    return passages;
  }

  /** Return the set of all retrieved document names. */
  public Set<String> resultSet() {
    HashSet<String> names = new HashSet<>();
    for (ScoredDocument sdoc : scoredDocuments) {
      names.add(sdoc.documentName);
    }
    return names;
  }

  @Override
  public boolean equals(Object o) {
    if(this == o) return true;
    if(o instanceof Results) {
      Results other = (Results) o;
      return this.scoredDocuments.equals(other.scoredDocuments);
    }
    return false;
  }
}

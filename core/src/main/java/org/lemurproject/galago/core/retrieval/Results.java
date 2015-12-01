/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import org.lemurproject.galago.core.retrieval.query.Node;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.*;

/**
 * Base results class Wrapper for a list of results, and provides some utility
 * functions for merging several results
 *
 * @author sjh
 */
public class Results implements Serializable {

  public transient Retrieval retrieval;
  public Node inputQuery;
  public List<ScoredDocument> scoredDocuments;
  public Class<? extends ProcessingModel> processingModel;

  // empty construction -- a
  public Results(Retrieval retrieval) {
    this.retrieval = retrieval;
    this.scoredDocuments = new ArrayList<>();
  }

  public Results(Retrieval retrieval, Node query) {
    this.retrieval = retrieval;
    this.inputQuery = query;
    this.scoredDocuments = new ArrayList<>();
  }

  public Results(Retrieval retrieval, Node query, List<ScoredDocument> scoredDocuments) {
    this.retrieval = retrieval;
    this.inputQuery = query;
    this.scoredDocuments = scoredDocuments;
  }

  /**
   * When performing passage retrieval, it's probably convenient to have this method.
   * @return a list of results, cast to ScoredPassages.
   */
  @Nonnull
  public List<ScoredPassage> asPassages() {
    ArrayList<ScoredPassage> passages = new ArrayList<>();
    for(ScoredDocument doc : scoredDocuments) {
      assert(doc instanceof ScoredPassage);
      passages.add((ScoredPassage) doc);
    }
    return passages;
  }

  /** Return the set of all retrieved document names. */
  @Nonnull
  public Set<String> resultSet() {
    HashSet<String> names = new HashSet<>();
    for (ScoredDocument sdoc : scoredDocuments) {
      names.add(sdoc.documentName);
    }
    return names;
  }

  /**
   * Treat this retrieval as a feature in another model; i.e. turn this ranked list into a map of (DocId -> Score).
   * @return Map of {@link ScoredDocument#documentName} to {@link ScoredDocument#score}
   */
  @Nonnull
  public Map<String,Double> asDocumentFeatures() {
    HashMap<String, Double> scores = new HashMap<>(scoredDocuments.size());
    for (ScoredDocument sdoc : scoredDocuments) {
      scores.put(sdoc.documentName, sdoc.score);
    }
    return scores;
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

  /**
   * Takes all the elements in this ranked list of results, and pulls the Document for them.
   * @param what the document-components object, tells whether you want text, terms, or metadata.
   * @return a mapping of Document identifier to Document objects
   * @throws IOException if the corpus doesn't exist or is corrupted somehow.
   */
  @Nonnull
  public Map<String, Document> pullDocuments(@Nonnull Document.DocumentComponents what) throws IOException {
    return retrieval.getDocuments(new ArrayList<>(this.resultSet()), what);
  }

  public void printToTrecrun(PrintStream trecrun, String qid, String system) {
    try {
      printToTrecrun(new PrintWriter(new OutputStreamWriter(trecrun, "UTF-8")), qid, system);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public void printToTrecrun(PrintWriter trecrun, String qid, String system) {
    for (ScoredDocument scoredDocument : this.scoredDocuments) {
      trecrun.println(scoredDocument.toTRECformat(qid, system));
    }
    trecrun.flush();
  }
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.MovableIndicatorIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Implements standard boolean processing model. Results are not ranked, just
 * returned.
 *
 *
 * @author irmarc
 */
public class SetModel extends ProcessingModel {

  LocalRetrieval retrieval;
  Index index;
  List<Integer> whitelist;

  public SetModel(LocalRetrieval lr) {
    retrieval = lr;
    this.index = retrieval.getIndex();
    whitelist = null;
  }

  @Override
  public void defineWorkingSet(List<Integer> docs) {
    Collections.sort(docs);
    whitelist = docs;
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    if (whitelist == null) {
      return executeWholeCollection(queryTree, queryParams);
    } else {
      return executeWorkingSet(queryTree, queryParams);
    }
  }

  private ScoredDocument[] executeWholeCollection(Node queryTree, Parameters queryParams)
          throws Exception {

    ScoringContext context = new ScoringContext();

    // construct the query iterators
    MovableIndicatorIterator iterator = (MovableIndicatorIterator) retrieval.createIterator(queryParams, queryTree, context);
    ArrayList<ScoredDocument> list = new ArrayList<ScoredDocument>();
    while (!iterator.isDone()) {

      // ensure we are at the document we wish to score
      // -- this function will move ALL iterators, 
      //     not just the ones that do not have all candidates
      iterator.syncTo(iterator.currentCandidate());

      if (iterator.hasMatch(iterator.currentCandidate())) {
        list.add(new ScoredDocument(iterator.currentCandidate(), 1.0));
      }
      iterator.movePast(iterator.currentCandidate());
    }
    return list.toArray(new ScoredDocument[0]);
  }

  private ScoredDocument[] executeWorkingSet(Node queryTree, Parameters queryParams)
          throws Exception {
    ScoringContext context = new ScoringContext();

    // construct the query iterators
    MovableIndicatorIterator iterator = (MovableIndicatorIterator) retrieval.createIterator(queryParams, queryTree, context);
    ArrayList<ScoredDocument> list = new ArrayList<ScoredDocument>();

    for (int i = 0; i < whitelist.size(); i++) {
      int document = whitelist.get(i);
      iterator.syncTo(document);
      if (iterator.hasMatch(document)) {
        list.add(new ScoredDocument(iterator.currentCandidate(), 1.0));
      }
    }
    return list.toArray(new ScoredDocument[0]);
  }
}

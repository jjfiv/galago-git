/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.ArrayList;
import java.util.Arrays;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.AbstractIndicator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Implements standard boolean processing model. Results are not ranked, just returned.
 *
 *
 * @author irmarc
 */
public class SetModel implements ProcessingModel {

  LocalRetrieval retrieval;
  Index index;
  int[] whitelist;

  public SetModel(LocalRetrieval lr) {
    retrieval = lr;
    this.index = retrieval.getIndex();
    whitelist = null;
  }

  @Override
  public void defineWorkingSet(int[] docs) {
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
    AbstractIndicator iterator = (AbstractIndicator) retrieval.createIterator(queryTree, context);
    ArrayList<ScoredDocument> list = new ArrayList<ScoredDocument>();
    while (!iterator.isDone()) {
      if (iterator.hasMatch(iterator.currentCandidate())) {
        list.add(new ScoredDocument(iterator.currentCandidate(), 1.0));
      }
      iterator.next();
    }
    return list.toArray(new ScoredDocument[0]);
  }

  private ScoredDocument[] executeWorkingSet(Node queryTree, Parameters queryParams)
          throws Exception {
    ScoringContext context = new ScoringContext();

    // have to be sure
    Arrays.sort(whitelist);

    // construct the query iterators
    AbstractIndicator iterator = (AbstractIndicator) retrieval.createIterator(queryTree, context);
    ArrayList<ScoredDocument> list = new ArrayList<ScoredDocument>();

    for (int i = 0; i < whitelist.length; i++) {
      int document = whitelist[i];
      iterator.moveTo(document);
      if (iterator.hasMatch(document)) {
        list.add(new ScoredDocument(iterator.currentCandidate(), 1.0));
      }
    }
    return list.toArray(new ScoredDocument[0]);
  }
}

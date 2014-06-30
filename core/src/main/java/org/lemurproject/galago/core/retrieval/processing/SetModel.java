/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.ArrayList;

import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.IndicatorIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.Parameters;

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

  public SetModel(LocalRetrieval lr) {
    retrieval = lr;
    this.index = retrieval.getIndex();
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    ScoringContext context = new ScoringContext();

    // construct the query iterators
    IndicatorIterator iterator =
            (IndicatorIterator) retrieval.createIterator(queryParams,
            queryTree);
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
}

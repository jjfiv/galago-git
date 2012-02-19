// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.util.Arrays;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.ScoreValueIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Performs straightforward document-at-a-time (daat) processing of a fully annotated query,
 * processing scores over documents. 
 *
 * @author irmarc
 */
public class RankedDocumentModel implements ProcessingModel {

  LocalRetrieval retrieval;
  Index index;
  int[] whitelist;

  public RankedDocumentModel(LocalRetrieval lr) {
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

  private ScoredDocument[] executeWorkingSet(Node queryTree, Parameters queryParams)
          throws Exception {
    // This model uses the simplest ScoringContext
    ScoringContext context = new ScoringContext();

    // have to be sure
    Arrays.sort(whitelist);

    // construct the query iterators
    ScoreValueIterator iterator = (ScoreValueIterator) retrieval.createIterator(queryTree, context);
    int requested = (int) queryParams.get("requested", 1000);

    // now there should be an iterator at the root of this tree
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>();
    LengthsReader.Iterator lengthsIterator = index.getLengthsIterator();

    for (int i = 0; i < whitelist.length; i++) {
      int document = whitelist[i];
      iterator.moveTo(document);
      lengthsIterator.skipToKey(document);
      int length = lengthsIterator.getCurrentLength();
      // This context is shared among all scorers
      context.document = document;
      context.length = length;
      double score = iterator.score();
      if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
        ScoredDocument scoredDocument = new ScoredDocument(document, score);
        queue.add(scoredDocument);
        if (requested > 0 && queue.size() > requested) {
          queue.poll();
        }
      }
    }
    return queue.toArray(new ScoredDocument[0]);
  }

  private ScoredDocument[] executeWholeCollection(Node queryTree, Parameters queryParams)
          throws Exception {

    // This model uses the simplest ScoringContext
    ScoringContext context = new ScoringContext();

    // Number of documents requested.
    int requested = (int) queryParams.get("requested", 1000);

    // Maintain a queue of candidates
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);

    // Need to maintain document lengths
    LengthsReader.Iterator lengthsIterator = index.getLengthsIterator();

    // construct the iterators -- we use tree processing
    ScoreValueIterator iterator = (ScoreValueIterator) retrieval.createIterator(queryTree, context);

    // now there should be an iterator at the root of this tree
    while (!iterator.isDone()) {
      int document = iterator.currentCandidate();
      lengthsIterator.skipToKey(document);
      int length = lengthsIterator.getCurrentLength();
      // This context is shared among all scorers
      context.document = document;
      context.length = length;
      if (iterator.hasMatch(document)) {
        double score = iterator.score();
        if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
          ScoredDocument scoredDocument = new ScoredDocument(document, score);
          queue.add(scoredDocument);
          if (requested > 0 && queue.size() > requested) {
            queue.poll();
          }
        }
      }
      iterator.next();
    }
    return queue.toArray(new ScoredDocument[0]);
  }
}

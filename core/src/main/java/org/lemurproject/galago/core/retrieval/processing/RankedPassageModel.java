// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.util.Arrays;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import org.lemurproject.galago.core.retrieval.iterator.MovableScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Performs passage-level retrieval scoring.
 *
 *
 * @author irmarc
 */
public class RankedPassageModel extends ProcessingModel {

  LocalRetrieval retrieval;
  Index index;
  int[] whitelist;

  public RankedPassageModel(LocalRetrieval lr) {
    this.retrieval = lr;
    this.index = lr.getIndex();
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

  public ScoredDocument[] executeWorkingSet(Node queryTree, Parameters queryParams)
          throws Exception {

    PassageScoringContext context = new PassageScoringContext();

    // have to be sure
    Arrays.sort(whitelist);

    // Following operations are all just setup
    int requested = (int) queryParams.get("requested", 1000);
    int passageSize = (int) queryParams.getLong("passageSize");
    int passageShift = (int) queryParams.getLong("passageShift");
    MovableScoreIterator iterator = (MovableScoreIterator) retrieval.createIterator(queryParams, queryTree, context);
    PriorityQueue<ScoredPassage> queue = new PriorityQueue<ScoredPassage>(requested);
    ProcessingModel.initializeLengths(retrieval, context);

    // now there should be an iterator at the root of this tree
    for (int i = 0; i < whitelist.length; i++) {
      int document = whitelist[i];
      // This context is shared among all scorers
      context.document = document;
      context.begin = 0;
      context.end = passageSize;

      iterator.moveTo(document);
      context.moveLengths(document);
      int length = context.getLength();


      // Keep iterating over the same doc, but incrementing the begin/end fields of the
      // context until the next one
      while (context.end <= length) {
        if (iterator.hasMatch(document)) {
          double score = iterator.score();
          if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
            ScoredPassage scored = new ScoredPassage(document, score, context.begin, context.end);
            queue.add(scored);
            if (requested > 0 && queue.size() > requested) {
              queue.poll();
            }
          }
        }

        // Move the window forward
        context.begin += passageShift;
        context.end += passageShift;
      }
    }
    return toReversedArray(queue);

  }

  public ScoredDocument[] executeWholeCollection(Node queryTree, Parameters queryParams)
          throws Exception {
    PassageScoringContext context = new PassageScoringContext();


    // Following operations are all just setup
    int requested = (int) queryParams.get("requested", 1000);
    int passageSize = (int) queryParams.getLong("passageSize");
    int passageShift = (int) queryParams.getLong("passageShift");
    MovableScoreIterator iterator = (MovableScoreIterator) retrieval.createIterator(queryParams, queryTree, context);
    ProcessingModel.initializeLengths(retrieval, context);
    PriorityQueue<ScoredPassage> queue = new PriorityQueue<ScoredPassage>(requested);

    // now there should be an iterator at the root of this tree
    while (!iterator.isDone()) {
      int document = iterator.currentCandidate();
      // This context is shared among all scorers
      context.document = document;
      context.begin = 0;
      context.end = passageSize;

      context.moveLengths(document);
      int length = context.getLength();


      // ensure we are at the document we wish to score
      // -- this function will move ALL iterators, 
      //     not just the ones that do not have all candidates
      iterator.moveTo(document);


      // Keep iterating over the same doc, but incrementing the begin/end fields of the
      // context until the next one
      while (context.end <= length) {
        if (iterator.hasMatch(document)) {
          double score = iterator.score();
          if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
            ScoredPassage scored = new ScoredPassage(document, score, context.begin, context.end);
            queue.add(scored);
            if (requested > 0 && queue.size() > requested) {
              queue.poll();
            }
          }
        }

        // Move the window forward
        context.begin += passageShift;
        context.end += passageShift;
      }
      iterator.movePast(document);
    }
    return toReversedArray(queue);
  }
}
